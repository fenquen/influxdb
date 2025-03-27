package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/ast"
	"github.com/influxdata/flux/csv"
	"github.com/influxdata/flux/iocounter"
	"github.com/influxdata/flux/lang"
	"github.com/influxdata/httprouter"
	"github.com/influxdata/influxdb/v2"
	pcontext "github.com/influxdata/influxdb/v2/context"
	"github.com/influxdata/influxdb/v2/http/metric"
	"github.com/influxdata/influxdb/v2/kit/check"
	"github.com/influxdata/influxdb/v2/kit/feature"
	"github.com/influxdata/influxdb/v2/kit/platform"
	errors2 "github.com/influxdata/influxdb/v2/kit/platform/errors"
	"github.com/influxdata/influxdb/v2/kit/tracing"
	kithttp "github.com/influxdata/influxdb/v2/kit/transport/http"
	"github.com/influxdata/influxdb/v2/logger"
	"github.com/influxdata/influxdb/v2/query"
	"github.com/influxdata/influxdb/v2/query/fluxlang"
	"github.com/pkg/errors"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	prefixQuery   = "/api/v2/query"
	traceIDHeader = "Trace-Id"
)

// FluxBackend is all services and associated parameters required to construct
// the FluxHandler.
type FluxBackend struct {
	errors2.HTTPErrorHandler
	log                *zap.Logger
	FluxLogEnabled     bool
	QueryEventRecorder metric.EventRecorder

	AlgoWProxy          FeatureProxyHandler
	OrganizationService influxdb.OrganizationService
	ProxyQueryService   query.ProxyQueryService
	FluxLanguageService fluxlang.FluxLanguageService
	Flagger             feature.Flagger
}

// NewFluxBackend returns a new instance of FluxBackend.
func NewFluxBackend(log *zap.Logger, b *APIBackend) *FluxBackend {
	return &FluxBackend{
		HTTPErrorHandler:    b.HTTPErrorHandler,
		log:                 log,
		FluxLogEnabled:      b.FluxLogEnabled,
		QueryEventRecorder:  b.QueryEventRecorder,
		AlgoWProxy:          b.AlgoWProxy,
		ProxyQueryService:   b.FluxService,
		OrganizationService: b.OrganizationService,
		FluxLanguageService: b.FluxLanguageService,
		Flagger:             b.Flagger,
	}
}

// HTTPDialect is an encoding dialect that can write metadata to HTTP headers
type HTTPDialect interface {
	SetHeaders(w http.ResponseWriter)
}

// FluxHandler implements handling flux queries.
type FluxHandler struct {
	*httprouter.Router
	errors2.HTTPErrorHandler
	log            *zap.Logger
	FluxLogEnabled bool

	Now                 func() time.Time
	OrganizationService influxdb.OrganizationService
	ProxyQueryService   query.ProxyQueryService // 其实是 ProxyQueryServiceAsyncBridge
	FluxLanguageService fluxlang.FluxLanguageService

	EventRecorder metric.EventRecorder

	Flagger feature.Flagger
}

// Prefix provides the route prefix.
func (*FluxHandler) Prefix() string {
	return prefixQuery
}

// NewFluxHandler returns a new handler at /api/v2/query for flux queries.
func NewFluxHandler(log *zap.Logger, b *FluxBackend) *FluxHandler {
	h := &FluxHandler{
		Router:           NewRouter(b.HTTPErrorHandler),
		Now:              time.Now,
		HTTPErrorHandler: b.HTTPErrorHandler,
		log:              log,
		FluxLogEnabled:   b.FluxLogEnabled,

		ProxyQueryService:   b.ProxyQueryService,
		OrganizationService: b.OrganizationService,
		EventRecorder:       b.QueryEventRecorder,
		FluxLanguageService: b.FluxLanguageService,
		Flagger:             b.Flagger,
	}

	// query reponses can optionally be gzip encoded
	qh := gziphandler.GzipHandler(http.HandlerFunc(h.handleQuery))
	h.Handler("POST", prefixQuery, withFeatureProxy(b.AlgoWProxy, qh))
	h.Handler("POST", "/api/v2/query/ast", withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.postFluxAST)))
	h.Handler("POST", "/api/v2/query/analyze", withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.postQueryAnalyze)))
	h.Handler("GET", "/api/v2/query/suggestions", withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.getFluxSuggestions)))
	h.Handler("GET", "/api/v2/query/suggestions/:name", withFeatureProxy(b.AlgoWProxy, http.HandlerFunc(h.getFluxSuggestion)))
	return h
}

func (fluxHandler *FluxHandler) handleQuery(httpResp http.ResponseWriter, httpReq *http.Request) {
	const op = "http/handlePostQuery"
	span, httpReq := tracing.ExtractFromHTTPRequest(httpReq, "FluxHandler")
	defer span.Finish()

	ctx := httpReq.Context()
	log := fluxHandler.log.With(logger.TraceFields(ctx)...)
	if id, _, found := tracing.InfoFromContext(ctx); found {
		httpResp.Header().Set(traceIDHeader, id)
	}

	// TODO(desa): I really don't like how we're recording the usage metrics here
	// Ideally this will be moved when we solve https://github.com/influxdata/influxdb/issues/13403
	var orgID platform.ID
	var requestBytes int
	sw := kithttp.NewStatusResponseWriter(httpResp)
	httpResp = sw
	defer func() {
		fluxHandler.EventRecorder.Record(ctx, metric.Event{
			OrgID:         orgID,
			Endpoint:      httpReq.URL.Path, // This should be sufficient for the time being as it should only be single endpoint.
			RequestBytes:  requestBytes,
			ResponseBytes: sw.ResponseBytes(),
			Status:        sw.Code(),
		})
	}()

	authorizer, err := pcontext.GetAuthorizer(ctx)
	if err != nil {
		err := &errors2.Error{
			Code: errors2.EUnauthorized,
			Msg:  "authorization is invalid or missing in the query request",
			Op:   op,
			Err:  err,
		}
		fluxHandler.HandleHTTPError(ctx, err, httpResp)
		return
	}

	proxyRequest, n, err := decodeProxyQueryRequest(ctx, httpReq, authorizer, fluxHandler.OrganizationService)
	if err != nil && err != influxdb.ErrAuthorizerNotSupported {
		err := &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "failed to decode request body",
			Op:   op,
			Err:  err,
		}
		fluxHandler.HandleHTTPError(ctx, err, httpResp)
		return
	}
	proxyRequest.Request.Source = httpReq.Header.Get("User-Agent")
	orgID = proxyRequest.Request.OrganizationID
	requestBytes = n

	// Transform the context into one with the request's authorization.
	ctx = pcontext.SetAuthorizer(ctx, proxyRequest.Request.Authorization)
	if fluxHandler.Flagger != nil {
		ctx, _ = feature.Annotate(ctx, fluxHandler.Flagger)
	}

	hd, ok := proxyRequest.Dialect.(HTTPDialect)
	if !ok {
		err := &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  fmt.Sprintf("unsupported dialect over HTTP: %T", proxyRequest.Dialect),
			Op:   op,
		}
		fluxHandler.HandleHTTPError(ctx, err, httpResp)
		return
	}
	hd.SetHeaders(httpResp)

	countableWriter := iocounter.Writer{Writer: httpResp}
	_, err = fluxHandler.ProxyQueryService.Query(ctx, &countableWriter, proxyRequest)
	if err != nil {
		if countableWriter.Count() == 0 {
			// Only record the error headers IFF nothing has been written to w.
			fluxHandler.HandleHTTPError(ctx, err, httpResp)
			return
		}
		_ = tracing.LogError(span, err)
		log.Info("Error writing response to client",
			zap.String("handler", "flux"),
			zap.Error(err),
		)
	}

	// Detailed logging for flux queries if enabled
	/*if fluxHandler.FluxLogEnabled {
		fluxHandler.logFluxQuery(cw.Count(), stats, req.Request.Compiler, err)
	}*/

}

func (fluxHandler *FluxHandler) logFluxQuery(n int64, stats flux.Statistics, compiler flux.Compiler, err error) {
	var q string
	fluxCompiler, ok := compiler.(lang.FluxCompiler)
	if !ok {
		q = "unknown"
	}
	q = fluxCompiler.Query

	fluxHandler.log.Info("Executed Flux query",
		zap.String("compiler_type", string(compiler.CompilerType())),
		zap.Int64("response_size", n),
		zap.String("query", q),
		zap.Error(err),
		zap.Duration("stat_total_duration", stats.TotalDuration),
		zap.Duration("stat_compile_duration", stats.CompileDuration),
		zap.Duration("stat_execute_duration", stats.ExecuteDuration),
		zap.Int64("stat_max_allocated", stats.MaxAllocated),
		zap.Int64("stat_total_allocated", stats.TotalAllocated),
	)
}

type langRequest struct {
	Query string `json:"query"`
}

type postFluxASTResponse struct {
	AST *ast.Package `json:"ast"`
}

// postFluxAST returns a flux AST for provided flux string
func (fluxHandler *FluxHandler) postFluxAST(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "FluxHandler")
	defer span.Finish()

	var request langRequest
	ctx := r.Context()

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		fluxHandler.HandleHTTPError(ctx, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "invalid json",
			Err:  err,
		}, w)
		return
	}

	pkg, err := query.Parse(fluxHandler.FluxLanguageService, request.Query)
	if err != nil {
		fluxHandler.HandleHTTPError(ctx, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "invalid AST",
			Err:  err,
		}, w)
		return
	}

	res := postFluxASTResponse{
		AST: pkg,
	}

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		logEncodingError(fluxHandler.log, r, err)
		return
	}
}

// postQueryAnalyze parses a query and returns any query errors.
func (fluxHandler *FluxHandler) postQueryAnalyze(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "FluxHandler")
	defer span.Finish()

	ctx := r.Context()

	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fluxHandler.HandleHTTPError(ctx, &errors2.Error{
			Code: errors2.EInvalid,
			Msg:  "invalid json",
			Err:  err,
		}, w)
		return
	}

	a, err := req.Analyze(fluxHandler.FluxLanguageService)
	if err != nil {
		fluxHandler.HandleHTTPError(ctx, err, w)
		return
	}
	if err := encodeResponse(ctx, w, http.StatusOK, a); err != nil {
		logEncodingError(fluxHandler.log, r, err)
		return
	}
}

// fluxParams contain flux funciton parameters as defined by the semantic graph
type fluxParams map[string]string

// suggestionResponse provides the parameters available for a given Flux function
type suggestionResponse struct {
	Name   string     `json:"name"`
	Params fluxParams `json:"params"`
}

// suggestionsResponse provides a list of available Flux functions
type suggestionsResponse struct {
	Functions []suggestionResponse `json:"funcs"`
}

// getFluxSuggestions returns a list of available Flux functions for the Flux Builder
func (fluxHandler *FluxHandler) getFluxSuggestions(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "FluxHandler")
	defer span.Finish()

	ctx := r.Context()
	completer := fluxHandler.FluxLanguageService.Completer()
	names := completer.FunctionNames()
	var functions []suggestionResponse
	for _, name := range names {
		suggestion, err := completer.FunctionSuggestion(name)
		if err != nil {
			fluxHandler.HandleHTTPError(ctx, err, w)
			return
		}

		filteredParams := make(fluxParams)
		for key, value := range suggestion.Params {
			if key == "table" {
				continue
			}

			filteredParams[key] = value
		}

		functions = append(functions, suggestionResponse{
			Name:   name,
			Params: filteredParams,
		})
	}
	res := suggestionsResponse{Functions: functions}

	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		logEncodingError(fluxHandler.log, r, err)
		return
	}
}

// getFluxSuggestion returns the function parameters for the requested function
func (fluxHandler *FluxHandler) getFluxSuggestion(w http.ResponseWriter, r *http.Request) {
	span, r := tracing.ExtractFromHTTPRequest(r, "FluxHandler")
	defer span.Finish()

	ctx := r.Context()
	name := httprouter.ParamsFromContext(ctx).ByName("name")
	completer := fluxHandler.FluxLanguageService.Completer()

	suggestion, err := completer.FunctionSuggestion(name)
	if err != nil {
		fluxHandler.HandleHTTPError(ctx, err, w)
		return
	}

	res := suggestionResponse{Name: name, Params: suggestion.Params}
	if err := encodeResponse(ctx, w, http.StatusOK, res); err != nil {
		logEncodingError(fluxHandler.log, r, err)
		return
	}
}

// PrometheusCollectors satisifies the prom.PrometheusCollector interface.
func (fluxHandler *FluxHandler) PrometheusCollectors() []prom.Collector {
	// TODO: gather and return relevant metrics.
	return nil
}

var _ query.ProxyQueryService = (*FluxService)(nil)

// FluxService connects to Influx via HTTP using tokens to run queries.
type FluxService struct {
	Addr               string
	Token              string
	Name               string
	InsecureSkipVerify bool
}

// Query runs a flux query against a influx server and sends the results to the io.Writer.
// Will use the token from the context over the token within the service struct.
func (s *FluxService) Query(ctx context.Context, w io.Writer, r *query.ProxyRequest) (flux.Statistics, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()
	u, err := NewURL(s.Addr, prefixQuery)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	params := url.Values{}
	params.Set(OrgID, r.Request.OrganizationID.String())
	u.RawQuery = params.Encode()

	qreq, err := QueryRequestFromProxyRequest(r)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qreq); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	SetToken(s.Token, hreq)

	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/csv")
	if r.Request.Source != "" {
		hreq.Header.Add("User-Agent", r.Request.Source)
	} else if s.Name != "" {
		hreq.Header.Add("User-Agent", s.Name)
	}

	// Now that the request is all set, we can apply header mutators.
	if err := r.Request.ApplyOptions(hreq.Header); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	hreq = hreq.WithContext(ctx)
	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	defer resp.Body.Close()

	if err := CheckError(resp); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}

	if _, err := io.Copy(w, resp.Body); err != nil {
		return flux.Statistics{}, tracing.LogError(span, err)
	}
	return flux.Statistics{}, nil
}

func (s FluxService) Check(ctx context.Context) check.Response {
	return QueryHealthCheck(s.Addr, s.InsecureSkipVerify)
}

var _ query.QueryService = (*FluxQueryService)(nil)

// FluxQueryService implements query.QueryService by making HTTP requests to the /api/v2/query API endpoint.
type FluxQueryService struct {
	Addr               string
	Token              string
	Name               string
	InsecureSkipVerify bool
}

// Query runs a flux query against a influx server and decodes the result
func (s *FluxQueryService) Query(ctx context.Context, r *query.Request) (flux.ResultIterator, error) {
	span, ctx := tracing.StartSpanFromContext(ctx)
	defer span.Finish()

	u, err := NewURL(s.Addr, prefixQuery)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	params := url.Values{}
	params.Set(OrgID, r.OrganizationID.String())
	u.RawQuery = params.Encode()

	preq := &query.ProxyRequest{
		Request: *r,
		Dialect: csv.DefaultDialect(),
	}
	qreq, err := QueryRequestFromProxyRequest(preq)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qreq); err != nil {
		return nil, tracing.LogError(span, err)
	}

	hreq, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	SetToken(s.Token, hreq)

	hreq.Header.Set("Content-Type", "application/json")
	hreq.Header.Set("Accept", "text/csv")
	if r.Source != "" {
		hreq.Header.Add("User-Agent", r.Source)
	} else if s.Name != "" {
		hreq.Header.Add("User-Agent", s.Name)
	}
	hreq = hreq.WithContext(ctx)

	// Now that the request is all set, we can apply header mutators.
	if err := r.ApplyOptions(hreq.Header); err != nil {
		return nil, tracing.LogError(span, err)
	}

	hc := NewClient(u.Scheme, s.InsecureSkipVerify)
	resp, err := hc.Do(hreq)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}
	// Can't defer resp.Body.Close here because the CSV decoder depends on reading from resp.Body after this function returns.

	if err := CheckError(resp); err != nil {
		return nil, tracing.LogError(span, err)
	}

	decoder := csv.NewMultiResultDecoder(csv.ResultDecoderConfig{})
	itr, err := decoder.Decode(resp.Body)
	if err != nil {
		return nil, tracing.LogError(span, err)
	}

	return itr, nil
}

func (s FluxQueryService) Check(ctx context.Context) check.Response {
	return QueryHealthCheck(s.Addr, s.InsecureSkipVerify)
}

// GetQueryResponse runs a flux query with common parameters and returns the response from the query service.
func GetQueryResponse(qr *QueryRequest, addr *url.URL, org, token string, headers ...string) (*http.Response, error) {
	if len(headers)%2 != 0 {
		return nil, fmt.Errorf("headers must be key value pairs")
	}
	u := *addr
	u.Path = prefixQuery
	params := url.Values{}
	params.Set(Org, org)
	u.RawQuery = params.Encode()

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(qr); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", u.String(), &body)
	if err != nil {
		return nil, err
	}

	SetToken(token, req)

	// Default headers.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/csv")
	// Apply custom headers.
	for i := 0; i < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}

	insecureSkipVerify := false
	hc := NewClient(u.Scheme, insecureSkipVerify)
	return hc.Do(req)
}

// GetQueryResponseBody reads the body of a response from some query service.
// It also checks for errors in the response.
func GetQueryResponseBody(res *http.Response) ([]byte, error) {
	if err := CheckError(res); err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return io.ReadAll(res.Body)
}

// SimpleQuery runs a flux query with common parameters and returns CSV results.
func SimpleQuery(addr *url.URL, flux, org, token string, headers ...string) ([]byte, error) {
	header := true
	qr := &QueryRequest{
		Type:  "flux",
		Query: flux,
		Dialect: QueryDialect{
			Header:         &header,
			Delimiter:      ",",
			CommentPrefix:  "#",
			DateTimeFormat: "RFC3339",
		},
	}
	res, err := GetQueryResponse(qr, addr, org, token, headers...)
	if err != nil {
		return nil, err
	}
	return GetQueryResponseBody(res)
}

func QueryHealthCheck(url string, insecureSkipVerify bool) check.Response {
	u, err := NewURL(url, "/health")
	if err != nil {
		return check.Response{
			Name:    "query health",
			Status:  check.StatusFail,
			Message: errors.Wrap(err, "could not form URL").Error(),
		}
	}

	hc := NewClient(u.Scheme, insecureSkipVerify)
	resp, err := hc.Get(u.String())
	if err != nil {
		return check.Response{
			Name:    "query health",
			Status:  check.StatusFail,
			Message: errors.Wrap(err, "error getting response").Error(),
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return check.Response{
			Name:    "query health",
			Status:  check.StatusFail,
			Message: fmt.Sprintf("http error %v", resp.StatusCode),
		}
	}

	var healthResponse check.Response
	if err = json.NewDecoder(resp.Body).Decode(&healthResponse); err != nil {
		return check.Response{
			Name:    "query health",
			Status:  check.StatusFail,
			Message: errors.Wrap(err, "error decoding JSON response").Error(),
		}
	}

	return healthResponse
}
