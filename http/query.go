package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/jsonweb"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
)

// QueryRequest is a flux query request.
type QueryRequest struct {
	Type  string `json:"type"`
	Query string `json:"query"`

	// Flux fields
	Extern  json.RawMessage `json:"extern,omitempty"`
	AST     json.RawMessage `json:"ast,omitempty"`
	Dialect QueryDialect    `json:"dialect"`
	Now     time.Time       `json:"now"`

	Org *influxdb.Organization `json:"-"`

	// PreferNoContent specifies if the Response to this request should
	// contain any result. This is done for avoiding unnecessary
	// bandwidth consumption in certain cases. For example, when the
	// query produces side effects and the results do not matter. E.g.:
	// 	from(...) |> ... |> to()
	// For example, tasks do not use the results of queries, but only
	// care about their side effects.
	// To obtain a QueryRequest with no result, add the header
	// `Prefer: return-no-content` to the HTTP request.
	PreferNoContent bool
	// PreferNoContentWithError is the same as above, but it forces the
	// Response to contain an error if that is a Flux runtime error encoded
	// in the response body.
	// To obtain a QueryRequest with no result but runtime errors,
	// add the header `Prefer: return-no-content-with-error` to the HTTP request.
	PreferNoContentWithError bool
}

// QueryDialect is the formatting options for the query response.
type QueryDialect struct {
	Header         *bool    `json:"header"`
	Delimiter      string   `json:"delimiter"`
	CommentPrefix  string   `json:"commentPrefix"`
	DateTimeFormat string   `json:"dateTimeFormat"`
	Annotations    []string `json:"annotations"`
}

// WithDefaults adds default values to the request.
func (queryRequest QueryRequest) WithDefaults() QueryRequest {
	if queryRequest.Type == "" {
		queryRequest.Type = "flux"
	}
	if queryRequest.Dialect.Delimiter == "" {
		queryRequest.Dialect.Delimiter = ","
	}
	if queryRequest.Dialect.DateTimeFormat == "" {
		queryRequest.Dialect.DateTimeFormat = "RFC3339"
	}
	if queryRequest.Dialect.Header == nil {
		header := true
		queryRequest.Dialect.Header = &header
	}
	return queryRequest
}

// Validate checks the query request and returns an error if the request is invalid.
func (queryRequest QueryRequest) Validate() error {
	if queryRequest.Query == "" && queryRequest.AST == nil {
		return errors.New(`request body requires either query or AST`)
	}

	if queryRequest.Type != "flux" {
		return fmt.Errorf(`unknown query type: %s`, queryRequest.Type)
	}

	if len(queryRequest.Dialect.CommentPrefix) > 1 {
		return fmt.Errorf("invalid dialect comment prefix: must be length 0 or 1")
	}

	if len(queryRequest.Dialect.Delimiter) != 1 {
		return fmt.Errorf("invalid dialect delimeter: must be length 1")
	}

	rune, size := utf8.DecodeRuneInString(queryRequest.Dialect.Delimiter)
	if rune == utf8.RuneError && size == 1 {
		return fmt.Errorf("invalid dialect delimeter character")
	}

	for _, a := range queryRequest.Dialect.Annotations {
		switch a {
		case "group", "datatype", "default":
		default:
			return fmt.Errorf(`unknown dialect annotation type: %s`, a)
		}
	}

	switch queryRequest.Dialect.DateTimeFormat {
	case "RFC3339", "RFC3339Nano":
	default:
		return fmt.Errorf(`unknown dialect date time format: %s`, queryRequest.Dialect.DateTimeFormat)
	}

	return nil
}

// QueryAnalysis is a structured response of errors.
type QueryAnalysis struct {
	Errors []queryParseError `json:"errors"`
}

type queryParseError struct {
	Line      int    `json:"line"`
	Column    int    `json:"column"`
	Character int    `json:"character"`
	Message   string `json:"message"`
}

// Analyze attempts to parse the query request and returns any errors
// encountered in a structured way.
func (queryRequest QueryRequest) Analyze(l fluxlang.FluxLanguageService) (*QueryAnalysis, error) {
	switch queryRequest.Type {
	case "flux":
		return queryRequest.analyzeFluxQuery(l)
	}

	return nil, fmt.Errorf("unknown query request type %s", queryRequest.Type)
}

func (queryRequest QueryRequest) analyzeFluxQuery(l fluxlang.FluxLanguageService) (*QueryAnalysis, error) {
	a := &QueryAnalysis{}
	pkg, err := query.Parse(l, queryRequest.Query)
	if pkg == nil {
		return nil, err
	}
	errCount := ast.Check(pkg)
	if errCount == 0 {
		a.Errors = []queryParseError{}
		return a, nil
	}
	a.Errors = make([]queryParseError, 0, errCount)
	ast.Walk(ast.CreateVisitor(func(node ast.Node) {
		loc := node.Location()
		for _, err := range node.Errs() {
			a.Errors = append(a.Errors, queryParseError{
				Line:    loc.Start.Line,
				Column:  loc.Start.Column,
				Message: err.Msg,
			})
		}
	}), pkg)
	return a, nil
}

// ProxyRequest returns a request to proxy from the flux.
func (queryRequest QueryRequest) ProxyRequest() (*query.ProxyRequest, error) {
	return queryRequest.proxyRequest(time.Now)
}

func (queryRequest QueryRequest) proxyRequest(now func() time.Time) (*query.ProxyRequest, error) {
	if err := queryRequest.Validate(); err != nil {
		return nil, err
	}

	n := queryRequest.Now
	if n.IsZero() {
		n = now()
	}

	// Query is preferred over AST
	var compiler flux.Compiler
	if queryRequest.Query != "" {
		switch queryRequest.Type {
		case "flux":
			fallthrough
		default:
			compiler = lang.FluxCompiler{
				Now:    n,
				Extern: queryRequest.Extern,
				Query:  queryRequest.Query,
			}
		}
	} else if len(queryRequest.AST) > 0 {
		c := lang.ASTCompiler{
			Extern: queryRequest.Extern,
			AST:    queryRequest.AST,
			Now:    n,
		}
		compiler = c
	}

	delimiter, _ := utf8.DecodeRuneInString(queryRequest.Dialect.Delimiter)

	noHeader := false
	if queryRequest.Dialect.Header != nil {
		noHeader = !*queryRequest.Dialect.Header
	}

	var dialect flux.Dialect
	if queryRequest.PreferNoContent {
		dialect = &query.NoContentDialect{}
	} else {
		// TODO(nathanielc): Use commentPrefix and dateTimeFormat
		// once they are supported.
		encConfig := csv.ResultEncoderConfig{
			NoHeader:    noHeader,
			Delimiter:   delimiter,
			Annotations: queryRequest.Dialect.Annotations,
		}
		if queryRequest.PreferNoContentWithError {
			dialect = &query.NoContentWithErrorDialect{
				ResultEncoderConfig: encConfig,
			}
		} else {
			dialect = &csv.Dialect{
				ResultEncoderConfig: encConfig,
			}
		}
	}

	return &query.ProxyRequest{
		Request: query.Request{
			OrganizationID: queryRequest.Org.ID,
			Compiler:       compiler,
		},
		Dialect: dialect,
	}, nil
}

// QueryRequestFromProxyRequest converts a query.ProxyRequest into a QueryRequest.
// The ProxyRequest must contain supported compilers and dialects otherwise an error occurs.
func QueryRequestFromProxyRequest(req *query.ProxyRequest) (*QueryRequest, error) {
	qr := new(QueryRequest)
	switch c := req.Request.Compiler.(type) {
	case lang.FluxCompiler:
		qr.Type = "flux"
		qr.Query = c.Query
		qr.Extern = c.Extern
		qr.Now = c.Now
	case lang.ASTCompiler:
		qr.Type = "flux"
		qr.AST = c.AST
		qr.Now = c.Now
	default:
		return nil, fmt.Errorf("unsupported compiler %T", c)
	}
	switch d := req.Dialect.(type) {
	case *csv.Dialect:
		var header = !d.ResultEncoderConfig.NoHeader
		qr.Dialect.Header = &header
		qr.Dialect.Delimiter = string(d.ResultEncoderConfig.Delimiter)
		qr.Dialect.CommentPrefix = "#"
		qr.Dialect.DateTimeFormat = "RFC3339"
		qr.Dialect.Annotations = d.ResultEncoderConfig.Annotations
	case *query.NoContentDialect:
		qr.PreferNoContent = true
	case *query.NoContentWithErrorDialect:
		qr.PreferNoContentWithError = true
	default:
		return nil, fmt.Errorf("unsupported dialect %T", d)
	}
	return qr, nil
}

const fluxContentType = "application/vnd.flux"

func decodeQueryRequest(ctx context.Context, r *http.Request, svc influxdb.OrganizationService) (*QueryRequest, int, error) {
	var queryRequest QueryRequest
	body := &countReader{Reader: r.Body}

	var contentType = "application/json"
	if ct := r.Header.Get("Content-Type"); ct != "" {
		contentType = ct
	}
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, body.readCount, err
	}
	switch mt {
	case fluxContentType:
		octets, err := io.ReadAll(body)
		if err != nil {
			return nil, body.readCount, err
		}
		queryRequest.Query = string(octets)
	case "application/json":
		fallthrough
	default:
		if err := json.NewDecoder(body).Decode(&queryRequest); err != nil {
			return nil, body.readCount,
				fmt.Errorf("failed parsing request body as JSON; if sending a raw Flux script, set 'Content-Type: %s' in your request headers: %w", fluxContentType, err)
		}
	}

	switch hv := r.Header.Get(query.PreferHeaderKey); hv {
	case query.PreferNoContentHeaderValue:
		queryRequest.PreferNoContent = true
	case query.PreferNoContentWErrHeaderValue:
		queryRequest.PreferNoContentWithError = true
	}

	queryRequest = queryRequest.WithDefaults()
	if err := queryRequest.Validate(); err != nil {
		return nil, body.readCount, err
	}

	queryRequest.Org, err = queryOrganization(ctx, r, svc)
	return &queryRequest, body.readCount, err
}

type countReader struct {
	readCount int
	io.Reader
}

func (r *countReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.readCount += n
	return n, err
}

func decodeProxyQueryRequest(ctx context.Context, httpReq *http.Request, authorizer influxdb.Authorizer, orgSvc influxdb.OrganizationService) (*query.ProxyRequest, int, error) {
	queryRequest, n, err := decodeQueryRequest(ctx, httpReq, orgSvc)
	if err != nil {
		return nil, n, err
	}

	proxyRequest, err := queryRequest.ProxyRequest()
	if err != nil {
		return nil, n, err
	}

	var authorization *influxdb.Authorization
	switch a := authorizer.(type) {
	case *influxdb.Authorization:
		authorization = a
	case *influxdb.Session:
		authorization = a.EphemeralAuth(queryRequest.Org.ID)
	case *jsonweb.Token:
		authorization = a.EphemeralAuth(queryRequest.Org.ID)
	default:
		return proxyRequest, n, influxdb.ErrAuthorizerNotSupported
	}

	proxyRequest.Request.Authorization = authorization
	return proxyRequest, n, nil
}
