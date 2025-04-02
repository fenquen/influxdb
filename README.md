# InfluxDB
<div align="center">
  <img  src="assets/influxdb-logo.png" width="600" alt="InfluxDB Logo">
</div>

<p align="center">
  <a href="https://circleci.com/gh/influxdata/influxdb">
  <img alt="CircleCI" src="https://circleci.com/gh/influxdata/influxdb.svg?style=svg" />
  </a>
  
  <a href="https://www.influxdata.com/slack">
  <img alt="Slack Status" src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" />
  </a>
  
  <a href="https://docs.influxdata.com/influxdb/v2.4/install/?t=Docker">
  <img alt="Docker Pulls" src="https://img.shields.io/docker/pulls/_/influxdb" />
  </a>
  
  <a href="https://github.com/influxdata/influxdb/blob/master/LICENSE">
  <img alt="Docker Pulls" src="https://img.shields.io/github/license/influxdata/influxdb" />
  </a>
</p>
<h3 align="center">
    <b><a href="https://www.influxdata.com/">Website</a></b>
    •
    <a href="https://docs.influxdata.com/">Documentation</a>
    •
    <a href="https://university.influxdata.com/">InfluxDB University</a>
    •
    <a href="https://www.influxdata.com/blog/">Blog</a>
</h3>

---

InfluxDB is an open source time series platform. This includes APIs for storing and querying data, processing it in the background for ETL or monitoring and alerting purposes, user dashboards, and visualizing and exploring the data and more. The master branch on this repo now represents the latest InfluxDB, which now includes functionality for Kapacitor (background processing) and Chronograf (the UI) all in a single binary.

The list of InfluxDB Client Libraries that are compatible with the latest version can be found in [our documentation](https://docs.influxdata.com/influxdb/latest/tools/client-libraries/).

If you are looking for the 1.x line of releases, there are branches for each minor version as well as a `master-1.x` branch that will contain the code for the next 1.x release. The master-1.x [working branch is here](https://github.com/influxdata/influxdb/tree/master-1.x). The [InfluxDB 1.x Go Client can be found here](https://github.com/influxdata/influxdb1-client).

| Try **InfluxDB Cloud** for free and get started fast with no local setup required. Click [**here**](https://cloud2.influxdata.com/signup) to start building your application on InfluxDB Cloud. |
|:------|

## Install

We have nightly and versioned Docker images, Debian packages, RPM packages, and tarballs of InfluxDB available at the [InfluxData downloads page](https://portal.influxdata.com/downloads/). We also provide the `influx` command line interface (CLI) client as a separate binary available at the same location.

If you are interested in building from source, see the [building from source](CONTRIBUTING.md#building-from-source) guide for contributors.

<a href="https://university.influxdata.com/catalog/">
  <img src="assets/influxdbU-banner.png" width="600"/>
</a>

## Get Started

For a complete getting started guide, please see our full [online documentation site](https://docs.influxdata.com/influxdb/latest/).

To write and query data or use the API in any way, you'll need to first create a user, credentials, organization and bucket.
Everything in InfluxDB is organized under a concept of an organization. The API is designed to be multi-tenant.
Buckets represent where you store time series data.
They're synonymous with what was previously in InfluxDB 1.x a database and retention policy.

The simplest way to get set up is to point your browser to [http://localhost:8086](http://localhost:8086) and go through the prompts.

You can also get set up from the CLI using the command `influx setup`:


```bash
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx setup
Welcome to InfluxDB 2.0!
Please type your primary username: marty

Please type your password:

Please type your password again:

Please type your primary organization name.: InfluxData

Please type your primary bucket name.: telegraf

Please type your retention period in hours.
Or press ENTER for infinite.: 72


You have entered:
  Username:          marty
  Organization:      InfluxData
  Bucket:            telegraf
  Retention Period:  72 hrs
Confirm? (y/n): y

UserID                  Username        Organization    Bucket
033a3f2c5ccaa000        marty           InfluxData      Telegraf
Your token has been stored in /Users/marty/.influxdbv2/credentials
```

You can run this command non-interactively using the `-f, --force` flag if you are automating the setup.
Some added flags can help:
```bash
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx setup \
--username marty \
--password F1uxKapacit0r85 \
--org InfluxData \
--bucket telegraf \
--retention 168 \
--token where-were-going-we-dont-need-roads \
--force
```

Once setup is complete, a configuration profile is created to allow you to interact with your local InfluxDB without passing in credentials each time. You can list and manage those profiles using the `influx config` command.
```bash
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx config
Active	Name	URL			            Org
*	    default	http://localhost:8086	InfluxData
```

## Write Data
Write to measurement `m`, with tag `v=2`, in bucket `telegraf`, which belongs to organization `InfluxData`:

```bash
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx write --bucket telegraf --precision s "m v=2 $(date +%s)"
```

Since you have a default profile set up, you can omit the Organization and Token from the command.

Write the same point using `curl`:

```bash
curl --header "Authorization: Token $(bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx auth list --json | jq -r '.[0].token')" \
--data-raw "m v=2 $(date +%s)" \
"http://localhost:8086/api/v2/write?org=InfluxData&bucket=telegraf&precision=s"
```

Read that back with a simple Flux query:

```bash
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx query 'from(bucket:"telegraf") |> range(start:-1h)'
Result: _result
Table: keys: [_start, _stop, _field, _measurement]
                   _start:time                      _stop:time           _field:string     _measurement:string                      _time:time                  _value:float
------------------------------  ------------------------------  ----------------------  ----------------------  ------------------------------  ----------------------------
2019-12-30T22:19:39.043918000Z  2019-12-30T23:19:39.043918000Z                       v                       m  2019-12-30T23:17:02.000000000Z                             2
```

Use the `-r, --raw` option to return the raw flux response from the query. This is useful for moving data from one instance to another as the `influx write` command can accept the Flux response using the `--format csv` option.

## Script with Flux

Flux (previously named IFQL) is an open source functional data scripting language designed for querying, analyzing, and acting on data. Flux supports multiple data source types, including:

- Time series databases (such as InfluxDB)
- Relational SQL databases (such as MySQL and PostgreSQL)
- CSV

The source for Flux is [available on GitHub](https://github.com/influxdata/flux).
To learn more about Flux, see the latest [InfluxData Flux documentation](https://docs.influxdata.com/flux/) and [CTO Paul Dix's presentation](https://speakerdeck.com/pauldix/flux-number-fluxlang-a-new-time-series-data-scripting-language).

## Contribute to the Project

InfluxDB is an [MIT licensed](LICENSE) open source project and we love our community. The fastest way to get something fixed is to open a PR. Check out our [contributing](CONTRIBUTING.md) guide if you're interested in helping out. Also, join us on our [Community Slack Workspace](https://influxdata.com/slack) if you have questions or comments for our engineering teams.

## CI and Static Analysis

### CI

All pull requests will run through CI, which is currently hosted by Circle.
Community contributors should be able to see the outcome of this process by looking at the checks on their PR.
Please fix any issues to ensure a prompt review from members of the team.

The InfluxDB project is used internally in a number of proprietary InfluxData products, and as such, PRs and changes need to be tested internally.
This can take some time, and is not really visible to community contributors.

### Static Analysis

This project uses the following static analysis tools.
Failure during the running of any of these tools results in a failed build.
Generally, code must be adjusted to satisfy these tools, though there are exceptions.

- [go vet](https://golang.org/cmd/vet/) checks for Go code that should be considered incorrect.
- [go fmt](https://golang.org/cmd/gofmt/) checks that Go code is correctly formatted.
- [go mod tidy](https://tip.golang.org/cmd/go/#hdr-Add_missing_and_remove_unused_modules) ensures that the source code and go.mod agree.
- [staticcheck](https://staticcheck.io/docs/) checks for things like: unused code, code that can be simplified, code that is incorrect and code that will have performance issues.

### staticcheck

If your PR fails `staticcheck` it is easy to dig into why it failed, and also to fix the problem.
First, take a look at the error message in Circle under the `staticcheck` build section, e.g.,

```
tsdb/tsm1/encoding.gen.go:1445:24: func BooleanValues.assertOrdered is unused (U1000)
tsdb/tsm1/encoding.go:172:7: receiver name should not be an underscore, omit the name if it is unused (ST1006)
```

Next, go and take a [look here](http://next.staticcheck.io/docs/checks) for some clarification on the error code that you have received, e.g., `U1000`.
The docs will tell you what's wrong, and often what you need to do to fix the issue.

#### Generated Code

Sometimes generated code will contain unused code or occasionally that will fail a different check.
`staticcheck` allows for [entire files](http://next.staticcheck.io/docs/#ignoring-problems) to be ignored, though it's not ideal.
A linter directive, in the form of a comment, must be placed within the generated file.
This is problematic because it will be erased if the file is re-generated.
Until a better solution comes about, below is the list of generated files that need an ignores comment.
If you re-generate a file and find that `staticcheck` has failed, please see this list below for what you need to put back:

|          File          |                             Comment                              |
| :--------------------: | :--------------------------------------------------------------: |
| query/promql/promql.go | //lint:file-ignore SA6001 Ignore all unused code, it's generated |

#### End-to-End Tests

CI also runs end-to-end tests. These test the integration between the `influxd` server the UI.
Since the UI is used by interal repositories as well as the `influxdb` repository, the
end-to-end tests cannot be run on forked pull requests or run locally. The extent of end-to-end
testing required for forked pull requests will be determined as part of the review process.

## Additional Resources
- [InfluxDB Tips and Tutorials](https://www.influxdata.com/blog/category/tech/influxdb/)
- [InfluxDB Essentials Course](https://university.influxdata.com/courses/influxdb-essentials-tutorial/)
- [Exploring InfluxDB Cloud Course](https://university.influxdata.com/courses/exploring-influxdb-cloud-tutorial/)





```text
InfluxDB 的存储架构可大致分为多个层级，
高到低依次为：组织（Organization）、存储桶（Bucket）、分片组（Shard Group）、分片（Shard）和存储引擎 engine
```
```xml
<project version="4">
  <component name="BookmarksManager">
    <option name="groups">
      <GroupState>
        <option name="bookmarks">
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/stdlib/influxdata/influxdb/source.go" />
              <entry key="line" value="23" />
            </attributes>
            <option name="description" value="execute.RegisterSource(ReadTagKeysPhysKind, createReadTagKeysSource)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/http/query_handler.go" />
              <entry key="line" value="197" />
            </attributes>
            <option name="description" value="fluxHandler.ProxyQueryService.Query()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/bridges.go" />
              <entry key="line" value="136" />
            </attributes>
            <option name="description" value="-query,err :=b.AsyncQueryService.Query()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/control/controller.go" />
              <entry key="line" value="248" />
            </attributes>
            <option name="description" value="--query, err := controller.query()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/control/controller.go" />
              <entry key="line" value="265" />
            </attributes>
            <option name="description" value="---controller.compileQuery(query, compiler)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/control/controller.go" />
              <entry key="line" value="271" />
            </attributes>
            <option name="description" value="---controller.enqueueQuery(query)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/control/controller.go" />
              <entry key="line" value="423" />
            </attributes>
            <option name="description" value="----controller.executeQuery(query)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/control/controller.go" />
              <entry key="line" value="487" />
            </attributes>
            <option name="description" value="-----fluxQuery, err := query.program.Start(ctx, query.memResAllocator)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/lang/compiler.go" />
              <entry key="line" value="506" />
            </attributes>
            <option name="description" value="------q, err := p.Program.Start(cctx, alloc)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/lang/compiler.go" />
              <entry key="line" value="306" />
            </attributes>
            <option name="description" value="-------resultMap, statsCh, err := e.Execute()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/execute/executor.go" />
              <entry key="line" value="76" />
            </attributes>
            <option name="description" value="--------es, err := e.createExecutionState()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/stdlib/influxdata/influxdb/source.go" />
              <entry key="line" value="173" />
            </attributes>
            <option name="description" value="bucketID, err := spec.LookupBucketID(ctx, orgID, deps.BucketLookup)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/dependency.go" />
              <entry key="line" value="29" />
            </attributes>
            <option name="description" value="bucket, err := b.BucketService.FindBucket(ctx, bucketFilter)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tenant/service_bucket.go" />
              <entry key="line" value="70" />
            </attributes>
            <option name="description" value="return s.FindBucketByName(ctx, *filter.OrganizationID, *filter.Name)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/execute/executor.go" />
              <entry key="line" value="80" />
            </attributes>
            <option name="description" value="--------es.do()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/execute/executor.go" />
              <entry key="line" value="534" />
            </attributes>
            <option name="description" value="---------src.Run(ctx)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/stdlib/influxdata/influxdb/source.go" />
              <entry key="line" value="49" />
            </attributes>
            <option name="description" value="----------err := s.runner.run(ctx)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/stdlib/influxdata/influxdb/source.go" />
              <entry key="line" value="137" />
            </attributes>
            <option name="description" value="-----------tableIterator, err := readFilterSource.storageReader.ReadFilter()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/stdlib/influxdata/influxdb/source.go" />
              <entry key="line" value="145" />
            </attributes>
            <option name="description" value="-----------readFilterSource.processTables(ctx, tableIterator, stop)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/stdlib/influxdata/influxdb/source.go" />
              <entry key="line" value="68" />
            </attributes>
            <option name="description" value="------------tableIterator.Do()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/storage/flux/reader.go" />
              <entry key="line" value="164" />
            </attributes>
            <option name="description" value="-------------resultSet, err := filterIterator.store.ReadFilter()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/services/storage/store.go" />
              <entry key="line" value="164" />
            </attributes>
            <option name="description" value="--------------shardIDs, err := store.findShardIDs(bucketIdStr, retentionPolicyName, false, start, end)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/services/storage/store.go" />
              <entry key="line" value="186" />
            </attributes>
            <option name="description" value="--------------reads.NewFilteredResultSet()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/storage/flux/reader.go" />
              <entry key="line" value="173" />
            </attributes>
            <option name="description" value="-------------filterIterator.handleRead(f, resultSet)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/storage/flux/reader.go" />
              <entry key="line" value="196" />
            </attributes>
            <option name="description" value="--------------cursor = resultSet.Cursor()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/storage/reads/resultset.go" />
              <entry key="line" value="60" />
            </attributes>
            <option name="description" value="---------------resultSet.multiShardCursors.createCursor(resultSet.seriesRow)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/storage/reads/array_cursor.go" />
              <entry key="line" value="117" />
            </attributes>
            <option name="description" value="----------------cursor, _ = shardCursorIter.Next()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor_iterator.go" />
              <entry key="line" value="64" />
            </attributes>
            <option name="description" value="-----------------arrayCursorIterator.buildFloatArrayCursor()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor_iterator.gen.go" />
              <entry key="line" value="20" />
            </attributes>
            <option name="description" value="------------------keyCursor := arrayCursorIterator.engine.KeyCursor()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
              <entry key="line" value="2334" />
            </attributes>
            <option name="description" value="-------------------engine.FileStore.KeyCursor()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/file_store.go" />
              <entry key="line" value="745" />
            </attributes>
            <option name="description" value="--------------------newKeyCursor()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/file_store.go" />
              <entry key="line" value="1363" />
            </attributes>
            <option name="description" value="---------------------fileStore.locations(key, t, ascending)," />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/storage/flux/reader.go" />
              <entry key="line" value="211" />
            </attributes>
            <option name="description" value="--------------storageTable = newFloatTable()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/query/stdlib/influxdata/influxdb/source.go" />
              <entry key="line" value="69" />
            </attributes>
            <option name="description" value="source.processTable(ctx, table)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/execute/result.go" />
              <entry key="line" value="46" />
            </attributes>
            <option name="description" value="case s.tables &lt;- resultMessage{" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/execute/result.go" />
              <entry key="line" value="70" />
            </attributes>
            <option name="description" value="if err := f(msg.table); err != nil {" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/storage/reads/array_cursor.gen.go" />
              <entry key="line" value="298" />
            </attributes>
            <option name="description" value="a := c.FloatArrayCursor.Next()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
        </option>
        <option name="name" value="query" />
      </GroupState>
      <GroupState>
        <option name="bookmarks">
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/http/write_handler.go" />
              <entry key="line" value="148" />
            </attributes>
            <option name="description" value="http读取写入得到writeReq" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/http/write_handler.go" />
              <entry key="line" value="184" />
            </attributes>
            <option name="description" value="parse写入的point" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/http/write_handler.go" />
              <entry key="line" value="191" />
            </attributes>
            <option name="description" value="写入点位" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/storage/points_writer.go" />
              <entry key="line" value="37" />
            </attributes>
            <option name="description" value="-写入点位" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/storage/engine.go" />
              <entry key="line" value="277" />
            </attributes>
            <option name="description" value="--写入点位" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/coordinator/points_writer.go" />
              <entry key="line" value="363" />
            </attributes>
            <option name="description" value="---写入点位" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/coordinator/points_writer.go" />
              <entry key="line" value="383" />
            </attributes>
            <option name="description" value="----map2Shards" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/coordinator/points_writer.go" />
              <entry key="line" value="238" />
            </attributes>
            <option name="description" value="-----CreateShardGroup" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/services/meta/client.go" />
              <entry key="line" value="726" />
            </attributes>
            <option name="description" value="------CreateShardGroupWithShards" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/services/meta/client.go" />
              <entry key="line" value="713" />
            </attributes>
            <option name="description" value="-------createShardGroup" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/services/meta/client.go" />
              <entry key="line" value="735" />
            </attributes>
            <option name="description" value="--------CreateShardGroup" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/coordinator/points_writer.go" />
              <entry key="line" value="251" />
            </attributes>
            <option name="description" value="-----定位到shardGroupInfo使用的是点位的time" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/coordinator/points_writer.go" />
              <entry key="line" value="259" />
            </attributes>
            <option name="description" value="----- 定位到shard使用的是点位的key(其实是measurement加上tags)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/coordinator/points_writer.go" />
              <entry key="line" value="392" />
            </attributes>
            <option name="description" value="----writeToShard" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/coordinator/points_writer.go" />
              <entry key="line" value="442" />
            </attributes>
            <option name="description" value="-----CreateShard" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/store.go" />
              <entry key="line" value="693" />
            </attributes>
            <option name="description" value="------openSeriesFile" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/store.go" />
              <entry key="line" value="707" />
            </attributes>
            <option name="description" value="------OpenShard" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/store.go" />
              <entry key="line" value="609" />
            </attributes>
            <option name="description" value="-------shard.Open(ctx)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/shard.go" />
              <entry key="line" value="368" />
            </attributes>
            <option name="description" value="--------shard.openNoLock(ctx)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/shard.go" />
              <entry key="line" value="408" />
            </attributes>
            <option name="description" value="---------index.Open()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/index/tsi1/index.go" />
              <entry key="line" value="287" />
            </attributes>
            <option name="description" value="----------partition.Open()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/shard.go" />
              <entry key="line" value="426" />
            </attributes>
            <option name="description" value="---------engine.Open(ctx)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/v1/coordinator/points_writer.go" />
              <entry key="line" value="447" />
            </attributes>
            <option name="description" value="-----WriteToShard" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/store.go" />
              <entry key="line" value="1595" />
            </attributes>
            <option name="description" value="------shard.WritePoints(ctx, points)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/shard.go" />
              <entry key="line" value="661" />
            </attributes>
            <option name="description" value="-------shard.validateSeriesAndFields(points)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/shard.go" />
              <entry key="line" value="678" />
            </attributes>
            <option name="description" value="-------engine.WritePoints(ctx, points)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
              <entry key="line" value="1351" />
            </attributes>
            <option name="description" value="--------写wal" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/wal.go" />
              <entry key="line" value="369" />
            </attributes>
            <option name="description" value="---------wal.writeToLog()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/wal.go" />
              <entry key="line" value="469" />
            </attributes>
            <option name="description" value="----------encode walEntry" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/wal.go" />
              <entry key="line" value="477" />
            </attributes>
            <option name="description" value="----------压缩 walEntry" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/wal.go" />
              <entry key="line" value="494" />
            </attributes>
            <option name="description" value="----------wal.rollSegmentIfNeed()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/wal.go" />
              <entry key="line" value="500" />
            </attributes>
            <option name="description" value="----------wal.currentSegmentWriter.Write()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/wal.go" />
              <entry key="line" value="510" />
            </attributes>
            <option name="description" value="----------wal.scheduleSync()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/wal.go" />
              <entry key="line" value="320" />
            </attributes>
            <option name="description" value="-----------syncDelay参数意义" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
        </option>
        <option name="name" value="write" />
      </GroupState>
      <GroupState>
        <option name="bookmarks">
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
              <entry key="line" value="751" />
            </attributes>
            <option name="description" value="engine.SetCompactionsEnabled(true)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
              <entry key="line" value="469" />
            </attributes>
            <option name="description" value="-engine.compactCache()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
              <entry key="line" value="1968" />
            </attributes>
            <option name="description" value="--engine.WriteSnapshot()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
              <entry key="line" value="1878" />
            </attributes>
            <option name="description" value="---engine.writeSnapshotAndCommit()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
              <entry key="line" value="1918" />
            </attributes>
            <option name="description" value="----newTsmFilePaths := engine.Compactor.WriteSnapshot()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/compact.go" />
              <entry key="line" value="860" />
            </attributes>
            <option name="description" value="-----cacheSnapshot.Split(concurrency)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/compact.go" />
              <entry key="line" value="871" />
            </attributes>
            <option name="description" value="-----compactor.writeNewTsmFiles()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/compact.go" />
              <entry key="line" value="1058" />
            </attributes>
            <option name="description" value="------compactor.writeNewTsmFile()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/compact.go" />
              <entry key="line" value="1125" />
            </attributes>
            <option name="description" value="-------tsmWriter:=NewTSMWriterWithDiskBuffer(limitWriter)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/compact.go" />
              <entry key="line" value="1177" />
            </attributes>
            <option name="description" value="-------tsmWriter.WriteBlock()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/writer.go" />
              <entry key="line" value="683" />
            </attributes>
            <option name="description" value="--------tsmWriter.indexWriter.Add()" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
          <BookmarkState>
            <attributes>
              <entry key="url" value="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
              <entry key="line" value="1928" />
            </attributes>
            <option name="description" value="----engine.FileStore.Replace(nil, newTsmFilePaths)" />
            <option name="provider" value="com.intellij.ide.bookmark.providers.LineBookmarkProvider" />
          </BookmarkState>
        </option>
        <option name="name" value="a" />
      </GroupState>
    </option>
  </component>
  <component name="BranchesTreeState">
    <expand>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="LOCAL" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:origin" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:adam" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:bugfix" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:cargo-deny" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:chore" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:ci" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:cli" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:core" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:core" type="80986c07:BranchNodeDescriptor$Group" />
        <item name="GROUP:compactor" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:core" type="80986c07:BranchNodeDescriptor$Group" />
        <item name="GROUP:load-generator" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:db" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:db" type="80986c07:BranchNodeDescriptor$Group" />
        <item name="GROUP:4201" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:db" type="80986c07:BranchNodeDescriptor$Group" />
        <item name="GROUP:5819" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:db" type="80986c07:BranchNodeDescriptor$Group" />
        <item name="GROUP:26110" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:dockerfile" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:docs" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:feat" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:fix" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:flux-staging" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:gw" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:gw" type="80986c07:BranchNodeDescriptor$Group" />
        <item name="GROUP:25559" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:gw" type="80986c07:BranchNodeDescriptor$Group" />
        <item name="GROUP:26188" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:gw" type="80986c07:BranchNodeDescriptor$Group" />
        <item name="GROUP:26189" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:gw" type="80986c07:BranchNodeDescriptor$Group" />
        <item name="GROUP:edge306" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:hiltontj" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:jdstrand" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:lesam" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:load-generator" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:mgattozzi" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:pbarnett" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:pd" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:pjb" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:pjb" type="80986c07:BranchNodeDescriptor$Group" />
        <item name="GROUP:25950" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:pr" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:prav" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:praveen" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:processing_engine" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:refactor" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:revert-25605-hiltontj" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:smith" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="REMOTE" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
        <item name="REMOTE:upstream" type="9a375a70:BranchNodeDescriptor$RemoteGroup" />
        <item name="GROUP:test" type="80986c07:BranchNodeDescriptor$Group" />
      </path>
      <path>
        <item name="ROOT" type="4b1c433e:BranchNodeDescriptor$Root" />
        <item name="TAG" type="7ab31ef7:BranchNodeDescriptor$TopLevelGroup" />
      </path>
    </expand>
    <select />
  </component>
  <component name="FileEditorManager">
    <leaf ideFingerprint="2el44m429u3x0" SIDE_TABS_SIZE_LIMIT_KEY="-1">
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/index/tsi1/partition.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="92">
              <caret line="709" column="28" selection-start-line="709" selection-start-column="28" selection-end-line="709" selection-end-column="28" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"partition.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/index/tsi1/index.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="268">
              <caret line="645" column="24" selection-start-line="645" selection-start-column="24" selection-end-line="645" selection-end-column="24" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"index.go","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file current-in-tab="true">
        <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="236">
              <caret line="469" selection-start-line="469" selection-end-line="469" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"engine.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/execute/result.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="636">
              <caret line="56" column="1" selection-start-line="56" selection-start-column="1" selection-end-line="56" selection-end-column="1" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"result.go","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/query/stdlib/influxdata/influxdb/source.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="236">
              <caret line="69" column="27" selection-start-line="69" selection-start-column="27" selection-end-line="69" selection-end-column="27" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"source.go","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/execute/executor.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="244">
              <caret line="76" column="27" selection-start-line="76" selection-start-column="27" selection-end-line="76" selection-end-column="27" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"executor.go","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/cache.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="244">
              <caret line="160" column="5" selection-start-line="160" selection-start-column="5" selection-end-line="160" selection-end-column="5" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"cache.go","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/shard.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="172">
              <caret line="656" column="24" selection-start-line="656" selection-start-column="24" selection-end-line="656" selection-end-column="24" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"shard.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/storage/reads/array_cursor.gen.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="252">
              <caret line="269" column="18" selection-start-line="269" selection-start-column="18" selection-end-line="269" selection-end-column="18" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"reads/array_cursor.gen.go","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor.gen.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="284">
              <caret line="48" selection-start-line="48" selection-end-line="48" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"tsm1/array_cursor.gen.go","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor_iterator.gen.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="284">
              <caret line="20" column="50" selection-start-line="20" selection-start-column="50" selection-end-line="20" selection-end-column="50" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"array_cursor_iterator.gen.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor_iterator.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="278">
              <caret line="64" column="53" selection-start-line="64" selection-start-column="53" selection-end-line="64" selection-end-column="53" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"array_cursor_iterator.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/storage/reads/array_cursor.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="236">
              <caret line="117" column="36" selection-start-line="117" selection-start-column="36" selection-end-line="117" selection-end-column="36" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"array_cursor.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/storage/reads/resultset.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="117">
              <caret line="60" selection-start-line="60" selection-end-line="60" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"resultset.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/storage/reads/series_cursor.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="236">
              <caret line="22" column="19" selection-start-line="22" selection-start-column="19" selection-end-line="22" selection-end-column="19" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"series_cursor.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/file_store.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="508">
              <caret line="958" column="10" selection-start-line="958" selection-start-column="10" selection-end-line="958" selection-end-column="10" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"file_store.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/compact.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="236">
              <caret line="860" selection-start-line="860" selection-end-line="860" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"compact.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/reader.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="236">
              <caret line="221" column="5" selection-start-line="221" selection-start-column="5" selection-end-line="221" selection-end-column="5" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"tsm1/reader.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/DESIGN.md">
          <provider editor-type-id="split-provider[text-editor;markdown-preview-editor]" selected="true">
            <state split_layout="SHOW_EDITOR" is_vertical_split="false">
              <first_editor relative-caret-position="-144" />
              <second_editor />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"DESIGN.md","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,96,10,24,105,99,111,110,115,47,77,97,114,107,100,111,119,110,80,108,117,103,105,110,46,115,118,103,18,24,105,99,111,110,115,47,101,120,112,117,105,47,109,97,114,107,100,111,119,110,46,115,118,103,26,29,111,114,103,46,105,110,116,101,108,108,105,106,46,112,108,117,103,105,110,115,46,109,97,114,107,100,111,119,110,40,-33,-55,-17,-63,-7,-1,-1,-1,-1,1,48,0]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/storage/reads/array_cursor.gen.go.tmpl">
          <provider editor-type-id="text-editor" selected="true" />
        </entry>
        <tab><![CDATA[{"tabTitle":"array_cursor.gen.go.tmpl","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,73,10,18,102,105,108,101,84,121,112,101,115,47,116,101,120,116,46,115,118,103,18,24,101,120,112,117,105,47,102,105,108,101,84,121,112,101,115,47,116,101,120,116,46,115,118,103,26,12,99,111,109,46,105,110,116,101,108,108,105,106,40,-122,-48,-19,-85,-2,-1,-1,-1,-1,1,48,0]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/tsdb/cursors/cursor.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="236">
              <caret line="70" column="4" selection-start-line="70" selection-start-column="4" selection-end-line="70" selection-end-column="4" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"cursor.go","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/storage/flux/reader.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="236">
              <caret line="211" selection-start-line="211" selection-end-line="211" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"flux/reader.go","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/storage/reads/store.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="166">
              <caret line="17" column="4" selection-start-line="17" selection-start-column="4" selection-end-line="17" selection-end-column="4" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"store.go","foregroundColor":-16777216,"textAttributes":{"name":"a"},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/storage/flux/table.gen.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="236">
              <caret line="38" column="5" selection-start-line="38" selection-start-column="5" selection-end-line="38" selection-end-column="5" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"table.gen.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
      <file>
        <entry file="file://$PROJECT_DIR$/storage/flux/table.go">
          <provider editor-type-id="text-editor" selected="true">
            <state relative-caret-position="252">
              <caret line="73" column="16" selection-start-line="73" selection-start-column="16" selection-end-line="73" selection-end-column="16" />
            </state>
          </provider>
        </entry>
        <tab><![CDATA[{"tabTitle":"table.go","foregroundColor":-16763981,"textAttributes":{"name":"a","children":[{"name":"option","attributes":{"name":"FOREGROUND","value":"33b3"}}]},"icon":[10,57,99,111,109,46,105,110,116,101,108,108,105,106,46,117,105,46,105,99,111,110,115,46,82,97,115,116,101,114,105,122,101,100,73,109,97,103,101,68,97,116,97,76,111,97,100,101,114,68,101,115,99,114,105,112,116,111,114,18,68,10,12,105,99,111,110,115,47,103,111,46,115,118,103,18,18,105,99,111,110,115,47,101,120,112,117,105,47,103,111,46,115,118,103,26,24,111,114,103,46,106,101,116,98,114,97,105,110,115,46,112,108,117,103,105,110,115,46,103,111,40,-57,-27,-111,-15,3,48,8]}]]></tab>
      </file>
    </leaf>
  </component>
  <component name="FileTypeUsageLocalSummary"><![CDATA[{
  "data": {
    "Go": {
      "usageCount": 160,
      "lastUsed": 1743592177632
    },
    "Markdown": {
      "usageCount": 7,
      "lastUsed": 1743591427415
    },
    "Shell Script": {
      "usageCount": 2,
      "lastUsed": 1743141316536
    },
    "XML": {
      "usageCount": 2,
      "lastUsed": 1743232056443
    },
    "x86 Plan9 Assembly": {
      "usageCount": 1,
      "lastUsed": 1743390926710
    },
    "protobuf": {
      "usageCount": 1,
      "lastUsed": 1743398506276
    },
    "PLAIN_TEXT": {
      "usageCount": 6,
      "lastUsed": 1743581717218
    }
  }
}]]></component>
  <component name="FindInProjectRecents">
    <findStrings>
      <find>no assets included in binary</find>
      <find>tsl</find>
      <find>createSeriesListIfNotExists</find>
      <find />
    </findStrings>
    <replaceStrings>
      <replace />
    </replaceStrings>
    <dirStrings>
      <dir>$PROJECT_DIR$/cmd/influxd</dir>
      <dir>$PROJECT_DIR$/tsdb/index/tsi1</dir>
      <dir>$PROJECT_DIR$/tsdb/index/tsi1/testdata/index-file-index/0</dir>
      <dir>$PROJECT_DIR$/tsdb/engine/tsm1</dir>
    </dirStrings>
  </component>
  <component name="FindInProjectScope">
    <option name="isProjectScope" value="true" />
  </component>
  <component name="GitSEFilterConfiguration">{}</component>
  <component name="IdeDocumentHistory"><![CDATA[{
  "changedPaths": [
    "/home/a/github/influxdb/README.md",
    "/home/a/github/influxdb/a.xml",
    "/home/a/github/influxdb/query/control/controller.go",
    "/home/a/github/influxdb/query/stdlib/influxdata/influxdb/operators.go",
    "/home/a/github/influxdb/query/stdlib/influxdata/influxdb/storage.go",
    "/home/a/github/influxdb/bucket.go",
    "/home/a/github/influxdb/authorizer/bucket.go",
    "/home/a/github/influxdb/query/dependency.go",
    "/home/a/github/influxdb/tenant/storage.go",
    "/home/a/github/influxdb/tenant/storage_bucket.go",
    "/home/a/github/influxdb/query/storage.go",
    "/home/a/github/influxdb/v1/services/meta/data.go",
    "/home/a/github/influxdb/tsdb/engine/tsm1/wal.go",
    "/home/a/github/influxdb/v1/services/meta/client.go",
    "/home/a/github/influxdb/v1/services/storage/store.go",
    "/home/a/github/influxdb/query/stdlib/influxdata/influxdb/source.go",
    "/home/a/github/influxdb/storage/flux/reader.go",
    "/home/a/github/influxdb/storage/flux/table.gen.go",
    "/home/a/github/influxdb/tsdb/store.go",
    "/home/a/github/influxdb/tsdb/series_file.go",
    "/home/a/github/influxdb/cmd/influxd/inspect/build_tsi/build_tsi.go",
    "/home/a/github/influxdb/tsdb/index.go",
    "/home/a/github/influxdb/tsdb/shard.go",
    "/home/a/github/influxdb/tsdb/index/tsi1/partition.go",
    "/home/a/github/influxdb/tsdb/engine/tsm1/array_cursor.gen.go",
    "/home/a/github/influxdb/tsdb/engine/tsm1/array_cursor_iterator.gen.go",
    "/home/a/github/influxdb/tsdb/engine/tsm1/array_cursor_iterator.go",
    "/home/a/github/influxdb/storage/reads/resultset.go",
    "/home/a/github/influxdb/storage/reads/series_cursor.go",
    "/home/a/github/influxdb/storage/reads/array_cursor.go",
    "/home/a/github/influxdb/tsdb/engine/tsm1/reader.go",
    "/home/a/github/influxdb/tsdb/engine/tsm1/file_store.go",
    "/home/a/github/influxdb/tsdb/engine/tsm1/engine.go"
  ]
}]]></component>
  <component name="IgnoredFileRootStore">
    <option name="generatedRoots">
      <set>
        <option value="$PROJECT_DIR$/.idea" />
      </set>
    </option>
  </component>
  <component name="LanguageUsageStatistics">
    <language id="Markdown">
      <summary usageCount="7" lastUsage="1743591427415" />
    </language>
    <language id="Shell Script">
      <summary usageCount="2" lastUsage="1743141316536" />
    </language>
    <language id="TEXT">
      <summary usageCount="6" lastUsage="1743581717218" />
    </language>
    <language id="XML">
      <summary usageCount="2" lastUsage="1743232056445" />
    </language>
    <language id="go">
      <summary usageCount="160" lastUsage="1743592177632" />
    </language>
    <language id="plan9_x86">
      <summary usageCount="1" lastUsage="1743390926710" />
    </language>
    <language id="protobuf">
      <summary usageCount="1" lastUsage="1743398506276" />
    </language>
  </component>
  <component name="ProjectView">
    <navigator currentView="ProjectPane" proportions="" version="1" />
    <panes>
      <pane id="ProjectPane">
        <subPane>
          <expand>
            <path>
              <item name="influxdb" type="b2602c69:ProjectViewProjectNode" />
              <item name="dir{file:///home/a/github/influxdb}" type="462c0819:PsiDirectoryNode" />
            </path>
            <path>
              <item name="influxdb" type="b2602c69:ProjectViewProjectNode" />
              <item name="dir{file:///home/a/github/influxdb}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/storage}" type="462c0819:PsiDirectoryNode" />
            </path>
            <path>
              <item name="influxdb" type="b2602c69:ProjectViewProjectNode" />
              <item name="dir{file:///home/a/github/influxdb}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/storage}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/storage/reads}" type="462c0819:PsiDirectoryNode" />
            </path>
            <path>
              <item name="influxdb" type="b2602c69:ProjectViewProjectNode" />
              <item name="dir{file:///home/a/github/influxdb}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/tsdb}" type="462c0819:PsiDirectoryNode" />
            </path>
            <path>
              <item name="influxdb" type="b2602c69:ProjectViewProjectNode" />
              <item name="dir{file:///home/a/github/influxdb}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/tsdb}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/tsdb/engine}" type="462c0819:PsiDirectoryNode" />
            </path>
            <path>
              <item name="influxdb" type="b2602c69:ProjectViewProjectNode" />
              <item name="dir{file:///home/a/github/influxdb}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/tsdb}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/tsdb/engine}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/tsdb/engine/tsm1}" type="462c0819:PsiDirectoryNode" />
            </path>
            <path>
              <item name="influxdb" type="b2602c69:ProjectViewProjectNode" />
              <item name="dir{file:///home/a/github/influxdb}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/tsdb}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/tsdb/engine}" type="462c0819:PsiDirectoryNode" />
              <item name="dir{file:///home/a/github/influxdb/tsdb/engine/tsm1}" type="462c0819:PsiDirectoryNode" />
              <item name="engine.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
            </path>
          </expand>
          <select />
          <presentation>
            <attributes />
            <data iconPath="expui/toolwindows/project.svg" iconPlugin="com.intellij" isLeaf="false" text="influxdb" />
            <item name="influxdb" type="b2602c69:ProjectViewProjectNode" />
            <presentation>
              <attributes>
                <map>
                  <entry key="filePath" value="$PROJECT_DIR$" />
                </map>
              </attributes>
              <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="influxdb" />
              <item name="dir{file:///home/a/github/influxdb}" type="462c0819:PsiDirectoryNode" />
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/.circleci" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text=".circleci" />
                <item name="dir{file:///home/a/github/influxdb/.circleci}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/.github" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text=".github" />
                <item name="dir{file:///home/a/github/influxdb/.github}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/annotations" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="annotations" />
                <item name="dir{file:///home/a/github/influxdb/annotations}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/assets" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="assets" />
                <item name="dir{file:///home/a/github/influxdb/assets}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/authorization" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="authorization" />
                <item name="dir{file:///home/a/github/influxdb/authorization}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/authorizer" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="authorizer" />
                <item name="dir{file:///home/a/github/influxdb/authorizer}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/backup" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="backup" />
                <item name="dir{file:///home/a/github/influxdb/backup}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/bin" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="bin" />
                <item name="dir{file:///home/a/github/influxdb/bin}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/bolt" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="bolt" />
                <item name="dir{file:///home/a/github/influxdb/bolt}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/checks" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="checks" />
                <item name="dir{file:///home/a/github/influxdb/checks}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/cmd" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="cmd" />
                <item name="dir{file:///home/a/github/influxdb/cmd}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/context" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="context" />
                <item name="dir{file:///home/a/github/influxdb/context}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/dashboards" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="dashboards" />
                <item name="dir{file:///home/a/github/influxdb/dashboards}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/dbrp" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="dbrp" />
                <item name="dir{file:///home/a/github/influxdb/dbrp}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/docker" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="docker" />
                <item name="dir{file:///home/a/github/influxdb/docker}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/etc" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="etc" />
                <item name="dir{file:///home/a/github/influxdb/etc}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/flux" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="flux" />
                <item name="dir{file:///home/a/github/influxdb/flux}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/fluxinit" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="fluxinit" />
                <item name="dir{file:///home/a/github/influxdb/fluxinit}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/gather" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="gather" />
                <item name="dir{file:///home/a/github/influxdb/gather}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/http" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="http" />
                <item name="dir{file:///home/a/github/influxdb/http}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/influxql" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="influxql" />
                <item name="dir{file:///home/a/github/influxdb/influxql}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/inmem" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="inmem" />
                <item name="dir{file:///home/a/github/influxdb/inmem}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/internal" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="internal" />
                <item name="dir{file:///home/a/github/influxdb/internal}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/jsonweb" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="jsonweb" />
                <item name="dir{file:///home/a/github/influxdb/jsonweb}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/kit" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="kit" />
                <item name="dir{file:///home/a/github/influxdb/kit}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/kv" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="kv" />
                <item name="dir{file:///home/a/github/influxdb/kv}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/label" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="label" />
                <item name="dir{file:///home/a/github/influxdb/label}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/logger" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="logger" />
                <item name="dir{file:///home/a/github/influxdb/logger}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/mock" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="mock" />
                <item name="dir{file:///home/a/github/influxdb/mock}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/models" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="models" />
                <item name="dir{file:///home/a/github/influxdb/models}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/notebooks" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="notebooks" />
                <item name="dir{file:///home/a/github/influxdb/notebooks}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/notification" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="notification" />
                <item name="dir{file:///home/a/github/influxdb/notification}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/pkg" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="pkg" />
                <item name="dir{file:///home/a/github/influxdb/pkg}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/pkger" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="pkger" />
                <item name="dir{file:///home/a/github/influxdb/pkger}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/pprof" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="pprof" />
                <item name="dir{file:///home/a/github/influxdb/pprof}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/predicate" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="predicate" />
                <item name="dir{file:///home/a/github/influxdb/predicate}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/prometheus" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="prometheus" />
                <item name="dir{file:///home/a/github/influxdb/prometheus}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/query" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="query" />
                <item name="dir{file:///home/a/github/influxdb/query}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/rand" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="rand" />
                <item name="dir{file:///home/a/github/influxdb/rand}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/releng" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="releng" />
                <item name="dir{file:///home/a/github/influxdb/releng}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/remotes" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="remotes" />
                <item name="dir{file:///home/a/github/influxdb/remotes}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/replications" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="replications" />
                <item name="dir{file:///home/a/github/influxdb/replications}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/resource" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="resource" />
                <item name="dir{file:///home/a/github/influxdb/resource}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/scripts" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="scripts" />
                <item name="dir{file:///home/a/github/influxdb/scripts}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/secret" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="secret" />
                <item name="dir{file:///home/a/github/influxdb/secret}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/session" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="session" />
                <item name="dir{file:///home/a/github/influxdb/session}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/snowflake" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="snowflake" />
                <item name="dir{file:///home/a/github/influxdb/snowflake}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/source" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="source" />
                <item name="dir{file:///home/a/github/influxdb/source}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/sqlite" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="sqlite" />
                <item name="dir{file:///home/a/github/influxdb/sqlite}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/static" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="static" />
                <item name="dir{file:///home/a/github/influxdb/static}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/storage" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="storage" />
                <item name="dir{file:///home/a/github/influxdb/storage}" type="462c0819:PsiDirectoryNode" />
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/storage/flux" />
                    </map>
                  </attributes>
                  <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="flux" />
                  <item name="dir{file:///home/a/github/influxdb/storage/flux}" type="462c0819:PsiDirectoryNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/storage/mocks" />
                    </map>
                  </attributes>
                  <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="mocks" />
                  <item name="dir{file:///home/a/github/influxdb/storage/mocks}" type="462c0819:PsiDirectoryNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/storage/reads" />
                    </map>
                  </attributes>
                  <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="reads" />
                  <item name="dir{file:///home/a/github/influxdb/storage/reads}" type="462c0819:PsiDirectoryNode" />
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/datatypes" />
                      </map>
                    </attributes>
                    <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="datatypes" />
                    <item name="dir{file:///home/a/github/influxdb/storage/reads/datatypes}" type="462c0819:PsiDirectoryNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/aggregate_resultset.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="aggregate_resultset.go" />
                    <item name="aggregate_resultset.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/array_cursor.gen.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="array_cursor.gen.go" />
                    <item name="array_cursor.gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/array_cursor.gen.go.tmpl" />
                      </map>
                    </attributes>
                    <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="array_cursor.gen.go.tmpl" />
                    <item name="array_cursor.gen.go.tmpl" type="ab9368bb:PsiFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/array_cursor.gen.go.tmpldata" />
                      </map>
                    </attributes>
                    <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="array_cursor.gen.go.tmpldata" />
                    <item name="array_cursor.gen.go.tmpldata" type="ab9368bb:PsiFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/array_cursor.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="array_cursor.go" />
                    <item name="array_cursor.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/array_cursor_gen_test.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/runTest.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="array_cursor_gen_test.go" />
                    <item name="array_cursor_gen_test.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/array_cursor_test.gen.go.tmpl" />
                      </map>
                    </attributes>
                    <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="array_cursor_test.gen.go.tmpl" />
                    <item name="array_cursor_test.gen.go.tmpl" type="ab9368bb:PsiFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/gen.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="true" text="gen.go" />
                    <item name="gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/group_resultset.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="group_resultset.go" />
                    <item name="group_resultset.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/influxql_eval.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="influxql_eval.go" />
                    <item name="influxql_eval.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/influxql_expr.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="influxql_expr.go" />
                    <item name="influxql_expr.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/influxql_predicate.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="influxql_predicate.go" />
                    <item name="influxql_predicate.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/keymerger.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="keymerger.go" />
                    <item name="keymerger.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/Makefile" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/makefile.svg" iconPlugin="name.kropp.intellij.makefile" isLeaf="true" text="Makefile" />
                    <item name="Makefile" type="ab9368bb:PsiFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/modulo.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="modulo.go" />
                    <item name="modulo.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/predicate.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="predicate.go" />
                    <item name="predicate.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/resultset.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="resultset.go" />
                    <item name="resultset.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/resultset_lineprotocol.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="resultset_lineprotocol.go" />
                    <item name="resultset_lineprotocol.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/series_cursor.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="series_cursor.go" />
                    <item name="series_cursor.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/store.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="store.go" />
                    <item name="store.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/tagsbuffer.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="tagsbuffer.go" />
                    <item name="tagsbuffer.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/storage/reads/types.tmpldata" />
                      </map>
                    </attributes>
                    <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="types.tmpldata" />
                    <item name="types.tmpldata" type="ab9368bb:PsiFileNode" />
                  </presentation>
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/storage/readservice" />
                    </map>
                  </attributes>
                  <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="readservice" />
                  <item name="dir{file:///home/a/github/influxdb/storage/readservice}" type="462c0819:PsiDirectoryNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/storage/bucket_service.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="bucket_service.go" />
                  <item name="bucket_service.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/storage/config.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="config.go" />
                  <item name="config.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/storage/engine.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="engine.go" />
                  <item name="engine.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/storage/points_writer.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="points_writer.go" />
                  <item name="points_writer.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/storage/retention.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="retention.go" />
                  <item name="retention.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/task" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="task" />
                <item name="dir{file:///home/a/github/influxdb/task}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/telegraf" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="telegraf" />
                <item name="dir{file:///home/a/github/influxdb/telegraf}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/telemetry" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="telemetry" />
                <item name="dir{file:///home/a/github/influxdb/telemetry}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/tenant" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="tenant" />
                <item name="dir{file:///home/a/github/influxdb/tenant}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/testing" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="testing" />
                <item name="dir{file:///home/a/github/influxdb/testing}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/tests" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="tests" />
                <item name="dir{file:///home/a/github/influxdb/tests}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/toml" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="toml" />
                <item name="dir{file:///home/a/github/influxdb/toml}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/tools" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="tools" />
                <item name="dir{file:///home/a/github/influxdb/tools}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/tsdb" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="tsdb" />
                <item name="dir{file:///home/a/github/influxdb/tsdb}" type="462c0819:PsiDirectoryNode" />
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/cursors" />
                    </map>
                  </attributes>
                  <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="cursors" />
                  <item name="dir{file:///home/a/github/influxdb/tsdb/cursors}" type="462c0819:PsiDirectoryNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine" />
                    </map>
                  </attributes>
                  <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="engine" />
                  <item name="dir{file:///home/a/github/influxdb/tsdb/engine}" type="462c0819:PsiDirectoryNode" />
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1" />
                      </map>
                    </attributes>
                    <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="tsm1" />
                    <item name="dir{file:///home/a/github/influxdb/tsdb/engine/tsm1}" type="462c0819:PsiDirectoryNode" />
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor.gen.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="array_cursor.gen.go" />
                      <item name="array_cursor.gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor.gen.go.tmpl" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="array_cursor.gen.go.tmpl" />
                      <item name="array_cursor.gen.go.tmpl" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor_iterator.gen.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="array_cursor_iterator.gen.go" />
                      <item name="array_cursor_iterator.gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor_iterator.gen.go.tmpl" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="array_cursor_iterator.gen.go.tmpl" />
                      <item name="array_cursor_iterator.gen.go.tmpl" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor_iterator.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="array_cursor_iterator.go" />
                      <item name="array_cursor_iterator.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/array_encoding.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="array_encoding.go" />
                      <item name="array_encoding.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/batch_boolean.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="batch_boolean.go" />
                      <item name="batch_boolean.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/batch_float.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="batch_float.go" />
                      <item name="batch_float.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/batch_integer.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="batch_integer.go" />
                      <item name="batch_integer.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/batch_string.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="batch_string.go" />
                      <item name="batch_string.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/batch_timestamp.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="batch_timestamp.go" />
                      <item name="batch_timestamp.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/bit_reader.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="bit_reader.go" />
                      <item name="bit_reader.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/bool.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="bool.go" />
                      <item name="bool.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/cache.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="cache.go" />
                      <item name="cache.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/compact.gen.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="compact.gen.go" />
                      <item name="compact.gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/compact.gen.go.tmpl" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="compact.gen.go.tmpl" />
                      <item name="compact.gen.go.tmpl" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/compact.gen.go.tmpldata" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="compact.gen.go.tmpldata" />
                      <item name="compact.gen.go.tmpldata" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/compact.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="compact.go" />
                      <item name="compact.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/DESIGN.md" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/markdown.svg" iconPlugin="org.intellij.plugins.markdown" isLeaf="true" text="DESIGN.md" />
                      <item name="DESIGN.md" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/digest.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="digest.go" />
                      <item name="digest.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/digest_reader.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="digest_reader.go" />
                      <item name="digest_reader.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/digest_writer.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="digest_writer.go" />
                      <item name="digest_writer.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/encoding.gen.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="encoding.gen.go" />
                      <item name="encoding.gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/encoding.gen.go.tmpl" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="encoding.gen.go.tmpl" />
                      <item name="encoding.gen.go.tmpl" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/encoding.gen.go.tmpldata" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="encoding.gen.go.tmpldata" />
                      <item name="encoding.gen.go.tmpldata" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/encoding.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="encoding.go" />
                      <item name="encoding.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.gen.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="engine.gen.go" />
                      <item name="engine.gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.gen.go.tmpl" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="engine.gen.go.tmpl" />
                      <item name="engine.gen.go.tmpl" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="engine.go" />
                      <item name="engine.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="_:tsdb.Engine" />
                        <item name="_:tsdb.Engine" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="addToIndexFromKey([][]byte, []influxql.DataType):error" />
                        <item name="addToIndexFromKey([][]byte, []influxql.DataType):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Apply()" />
                        <item name="Apply()" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Backup(io.Writer, string, time.Time):error" />
                        <item name="Backup(io.Writer, string, time.Time):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="blockToFieldType:[8]influxql.DataType" />
                        <item name="blockToFieldType:[8]influxql.DataType" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="BlockTypeToInfluxQLDataType(byte):influxql.DataType" />
                        <item name="BlockTypeToInfluxQLDataType(byte):influxql.DataType" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="buildCursor(context.Context, string, string, models.Tags, *influxql.VarRef, query.IteratorOptions):cursor" />
                        <item name="buildCursor(context.Context, string, string, models.Tags, *influxql.VarRef, query.IteratorOptions):cursor" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="cleanup():error" />
                        <item name="cleanup():error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="cleanupMeasurement([]byte):(bool, error)" />
                        <item name="cleanupMeasurement([]byte):(bool, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="cleanupTempTSMFiles():error" />
                        <item name="cleanupTempTSMFiles():error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Close():error" />
                        <item name="Close():error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="compact(*sync.WaitGroup)" />
                        <item name="compact(*sync.WaitGroup)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="compactCache()" />
                        <item name="compactCache()" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="compactFull(CompactionGroup, *sync.WaitGroup):bool" />
                        <item name="compactFull(CompactionGroup, *sync.WaitGroup):bool" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="compactGroup()" />
                        <item name="compactGroup()" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/type.svg" iconPlugin="com.intellij" isLeaf="false" text="compactionCounter" />
                        <item name="compactionCounter" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/type.svg" iconPlugin="com.intellij" isLeaf="false" text="compactionMetrics" />
                        <item name="compactionMetrics" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/type.svg" iconPlugin="com.intellij" isLeaf="false" text="compactionStrategy" />
                        <item name="compactionStrategy" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="compactLevel(CompactionGroup, int, bool, *sync.WaitGroup):bool" />
                        <item name="compactLevel(CompactionGroup, int, bool, *sync.WaitGroup):bool" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="countForLevel(int):*int64" />
                        <item name="countForLevel(int):*int64" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="createCallIterator(context.Context, string, *influxql.Call, query.IteratorOptions):([]query.Iterator, error)" />
                        <item name="createCallIterator(context.Context, string, *influxql.Call, query.IteratorOptions):([]query.Iterator, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="CreateIterator(context.Context, string, query.IteratorOptions):(query.Iterator, error)" />
                        <item name="CreateIterator(context.Context, string, query.IteratorOptions):(query.Iterator, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="CreateSeriesIfNotExists([]byte, []byte, models.Tags):error" />
                        <item name="CreateSeriesIfNotExists([]byte, []byte, models.Tags):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="createSeriesIterator(string, *influxql.VarRef, tsdb.IndexSet, query.IteratorOptions):(query.Iterator, error)" />
                        <item name="createSeriesIterator(string, *influxql.VarRef, tsdb.IndexSet, query.IteratorOptions):(query.Iterator, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="CreateSeriesListIfNotExists([][]byte, [][]byte, []models.Tags):error" />
                        <item name="CreateSeriesListIfNotExists([][]byte, [][]byte, []models.Tags):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="CreateSnapshot(bool):(string, error)" />
                        <item name="CreateSnapshot(bool):(string, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="createTagSetGroupIterators(context.Context, *influxql.VarRef, string, []string, *query.TagSet, []influxql.Expr, query.IteratorOptions):([]query.Iterator, error)" />
                        <item name="createTagSetGroupIterators(context.Context, *influxql.VarRef, string, []string, *query.TagSet, []influxql.Expr, query.IteratorOptions):([]query.Iterator, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="createTagSetIterators(context.Context, *influxql.VarRef, string, *query.TagSet, query.IteratorOptions):([]query.Iterator, error)" />
                        <item name="createTagSetIterators(context.Context, *influxql.VarRef, string, *query.TagSet, query.IteratorOptions):([]query.Iterator, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="createVarRefIterator(context.Context, string, query.IteratorOptions):([]query.Iterator, error)" />
                        <item name="createVarRefIterator(context.Context, string, query.IteratorOptions):([]query.Iterator, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="createVarRefSeriesIterator(context.Context, *influxql.VarRef, string, string, *query.TagSet, influxql.Expr, []influxql.VarRef, query.IteratorOptions):(query.Iterator, error)" />
                        <item name="createVarRefSeriesIterator(context.Context, *influxql.VarRef, string, string, *query.TagSet, influxql.Expr, []influxql.VarRef, query.IteratorOptions):(query.Iterator, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="deleteFlushThreshold:untyped int" />
                        <item name="deleteFlushThreshold:untyped int" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="DeleteMeasurement(context.Context, []byte):error" />
                        <item name="DeleteMeasurement(context.Context, []byte):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="deleteSeriesRange(context.Context, [][]byte, int64, int64):error" />
                        <item name="deleteSeriesRange(context.Context, [][]byte, int64, int64):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="DeleteSeriesRange(context.Context, tsdb.SeriesIterator, int64, int64):error" />
                        <item name="DeleteSeriesRange(context.Context, tsdb.SeriesIterator, int64, int64):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="DeleteSeriesRangeWithPredicate(context.Context, tsdb.SeriesIterator, func([]byte, models.Tags) (int64, int64, bool)):error" />
                        <item name="DeleteSeriesRangeWithPredicate(context.Context, tsdb.SeriesIterator, func([]byte, models.Tags) (int64, int64, bool)):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Digest():(io.ReadCloser, int64, error)" />
                        <item name="Digest():(io.ReadCloser, int64, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="disableLevelCompactions(bool)" />
                        <item name="disableLevelCompactions(bool)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="disableSnapshotCompactions()" />
                        <item name="disableSnapshotCompactions()" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="DiskSize():int64" />
                        <item name="DiskSize():int64" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="emptyBytes:[]byte" />
                        <item name="emptyBytes:[]byte" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="enableLevelCompactions(bool)" />
                        <item name="enableLevelCompactions(bool)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="enableSnapshotCompactions()" />
                        <item name="enableSnapshotCompactions()" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/type.svg" iconPlugin="com.intellij" isLeaf="false" text="Engine" />
                        <item name="Engine" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="engineSubsystem:string" />
                        <item name="engineSubsystem:string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Export(io.Writer, string, time.Time, time.Time):error" />
                        <item name="Export(io.Writer, string, time.Time, time.Time):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="filterFileToBackup(*TsmFileReader, os.FileInfo, string, string, int64, int64, *tar.Writer):error" />
                        <item name="filterFileToBackup(*TsmFileReader, os.FileInfo, string, string, int64, int64, *tar.Writer):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="ForEachMeasurementName(func([]byte) error):error" />
                        <item name="ForEachMeasurementName(func([]byte) error):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Free():error" />
                        <item name="Free():error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="fullCompactionStrategy(CompactionGroup, bool):*compactionStrategy" />
                        <item name="fullCompactionStrategy(CompactionGroup, bool):*compactionStrategy" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="globalCompactionMetrics:*compactionMetrics" />
                        <item name="globalCompactionMetrics:*compactionMetrics" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="HasTagKey([]byte, []byte):(bool, error)" />
                        <item name="HasTagKey([]byte, []byte):(bool, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Import(io.Reader, string):error" />
                        <item name="Import(io.Reader, string):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="init()" />
                        <item name="init()" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="IsIdle():(bool, string)" />
                        <item name="IsIdle():(bool, string)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="IteratorCost(string, query.IteratorOptions):(query.IteratorCost, error)" />
                        <item name="IteratorCost(string, query.IteratorOptions):(query.IteratorCost, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="KeyCursor(context.Context, []byte, int64, bool):*KeyCursor" />
                        <item name="KeyCursor(context.Context, []byte, int64, bool):*KeyCursor" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="keyFieldSeparator:string" />
                        <item name="keyFieldSeparator:string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="keyFieldSeparatorBytes:[]byte" />
                        <item name="keyFieldSeparatorBytes:[]byte" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="labelForLevel(int):prometheus.Labels" />
                        <item name="labelForLevel(int):prometheus.Labels" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="LastModified():time.Time" />
                        <item name="LastModified():time.Time" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="level1:string" />
                        <item name="level1:string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="level2:string" />
                        <item name="level2:string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="level3:string" />
                        <item name="level3:string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="levelCache:string" />
                        <item name="levelCache:string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="levelCompactionStrategy(CompactionGroup, bool, int):*compactionStrategy" />
                        <item name="levelCompactionStrategy(CompactionGroup, bool, int):*compactionStrategy" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="levelFull:string" />
                        <item name="levelFull:string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="levelKey:string" />
                        <item name="levelKey:string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="levelOpt:string" />
                        <item name="levelOpt:string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="LoadMetadataIndex(uint64, tsdb.Index):error" />
                        <item name="LoadMetadataIndex(uint64, tsdb.Index):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="matchTagValues(models.Tags, influxql.Expr):[]string" />
                        <item name="matchTagValues(models.Tags, influxql.Expr):[]string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="MeasurementExists([]byte):(bool, error)" />
                        <item name="MeasurementExists([]byte):(bool, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="MeasurementFields([]byte):*tsdb.MeasurementFields" />
                        <item name="MeasurementFields([]byte):*tsdb.MeasurementFields" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="MeasurementFieldSet():*tsdb.MeasurementFieldSet" />
                        <item name="MeasurementFieldSet():*tsdb.MeasurementFieldSet" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="MeasurementNamesByRegex(*regexp.Regexp):([][]byte, error)" />
                        <item name="MeasurementNamesByRegex(*regexp.Regexp):([][]byte, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="MeasurementsSketches():(estimator.Sketch, estimator.Sketch, error)" />
                        <item name="MeasurementsSketches():(estimator.Sketch, estimator.Sketch, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="MeasurementTagKeysByExpr([]byte, influxql.Expr):(map[string]struct{}, error)" />
                        <item name="MeasurementTagKeysByExpr([]byte, influxql.Expr):(map[string]struct{}, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="newAllCompactionMetrics([]string):*compactionMetrics" />
                        <item name="newAllCompactionMetrics([]string):*compactionMetrics" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="NewEngine(uint64, tsdb.Index, string, string, *tsdb.SeriesFile, tsdb.EngineOptions):tsdb.Engine" />
                        <item name="NewEngine(uint64, tsdb.Index, string, string, *tsdb.SeriesFile, tsdb.EngineOptions):tsdb.Engine" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="newEngineMetrics(tsdb.EngineTags):*compactionMetrics" />
                        <item name="newEngineMetrics(tsdb.EngineTags):*compactionMetrics" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="numberOfAuxCursorsCounter:ID" />
                        <item name="numberOfAuxCursorsCounter:ID" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="numberOfCondCursorsCounter:ID" />
                        <item name="numberOfCondCursorsCounter:ID" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="numberOfRefCursorsCounter:ID" />
                        <item name="numberOfRefCursorsCounter:ID" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Open(context.Context):error" />
                        <item name="Open(context.Context):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="overlay(io.Reader, string, bool):error" />
                        <item name="overlay(io.Reader, string, bool):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Path():string" />
                        <item name="Path():string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="planningTimer:ID" />
                        <item name="planningTimer:ID" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="PrometheusCollectors():[]prometheus.Collector" />
                        <item name="PrometheusCollectors():[]prometheus.Collector" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="readFileFromBackup(*tar.Reader, string, bool):(string, error)" />
                        <item name="readFileFromBackup(*tar.Reader, string, bool):(string, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Reindex():error" />
                        <item name="Reindex():error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="reindexBatchSize:untyped int" />
                        <item name="reindexBatchSize:untyped int" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="reloadCache():error" />
                        <item name="reloadCache():error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Restore(io.Reader, string):error" />
                        <item name="Restore(io.Reader, string):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="ScheduleFullCompaction():error" />
                        <item name="ScheduleFullCompaction():error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="SeriesAndFieldFromCompositeKey([]byte):([]byte, []byte)" />
                        <item name="SeriesAndFieldFromCompositeKey([]byte):([]byte, []byte)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="seriesCost(string, string, int64, int64):query.IteratorCost" />
                        <item name="seriesCost(string, string, int64, int64):query.IteratorCost" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="SeriesFieldKey(string, string):string" />
                        <item name="SeriesFieldKey(string, string):string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="SeriesFieldKeyBytes(string, string):[]byte" />
                        <item name="SeriesFieldKeyBytes(string, string):[]byte" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="SeriesN():int64" />
                        <item name="SeriesN():int64" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="SeriesSketches():(estimator.Sketch, estimator.Sketch, error)" />
                        <item name="SeriesSketches():(estimator.Sketch, estimator.Sketch, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="SetCompactionsEnabled(bool)" />
                        <item name="SetCompactionsEnabled(bool)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="SetEnabled(bool)" />
                        <item name="SetEnabled(bool)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="ShouldCompactCache(time.Time):bool" />
                        <item name="ShouldCompactCache(time.Time):bool" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/constant.svg" iconPlugin="com.intellij" isLeaf="true" text="storageNamespace:string" />
                        <item name="storageNamespace:string" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="TagKeyCardinality([]byte, []byte):int" />
                        <item name="TagKeyCardinality([]byte, []byte):int" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="timeBytes:[]byte" />
                        <item name="timeBytes:[]byte" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="timeStampFilterTarFile(time.Time, time.Time):func(os.FileInfo, string, string, *tar.Writer) error" />
                        <item name="timeStampFilterTarFile(time.Time, time.Time):func(os.FileInfo, string, string, *tar.Writer) error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/variable.svg" iconPlugin="com.intellij" isLeaf="true" text="tsmGroup:GID" />
                        <item name="tsmGroup:GID" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="Type([]byte):(models.FieldType, error)" />
                        <item name="Type([]byte):(models.FieldType, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="varRefSliceContains([]influxql.VarRef, string):bool" />
                        <item name="varRefSliceContains([]influxql.VarRef, string):bool" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/function.svg" iconPlugin="com.intellij" isLeaf="true" text="varRefSliceRemove([]influxql.VarRef, string):[]influxql.VarRef" />
                        <item name="varRefSliceRemove([]influxql.VarRef, string):[]influxql.VarRef" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="WithFormatFileNameFunc(FormatFileNameFunc)" />
                        <item name="WithFormatFileNameFunc(FormatFileNameFunc)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="WithLogger(*zap.Logger)" />
                        <item name="WithLogger(*zap.Logger)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="WithParseFileNameFunc(ParseFileNameFunc)" />
                        <item name="WithParseFileNameFunc(ParseFileNameFunc)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="WritePoints(context.Context, []models.Point):error" />
                        <item name="WritePoints(context.Context, []models.Point):error" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="WriteSnapshot():(error)" />
                        <item name="WriteSnapshot():(error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="writeSnapshotAndCommit(*zap.Logger, []string, *Cache):(error)" />
                        <item name="writeSnapshotAndCommit(*zap.Logger, []string, *Cache):(error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                      <presentation>
                        <attributes>
                          <map>
                            <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine.go" />
                          </map>
                        </attributes>
                        <data iconPath="expui/nodes/method.svg" iconPlugin="com.intellij" isLeaf="true" text="WriteTo(io.Writer):(int64, error)" />
                        <item name="WriteTo(io.Writer):(int64, error)" type="b598f48:GoTreeStructureProvider$GoElementNode" />
                      </presentation>
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/engine_cursor.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="engine_cursor.go" />
                      <item name="engine_cursor.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/file_store.gen.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="file_store.gen.go" />
                      <item name="file_store.gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/file_store.gen.go.tmpl" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="file_store.gen.go.tmpl" />
                      <item name="file_store.gen.go.tmpl" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/file_store.gen.go.tmpldata" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/text.svg" iconPlugin="com.intellij" isLeaf="true" text="file_store.gen.go.tmpldata" />
                      <item name="file_store.gen.go.tmpldata" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/file_store.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="file_store.go" />
                      <item name="file_store.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/file_store_array.gen.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="file_store_array.gen.go" />
                      <item name="file_store_array.gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/file_store_key_iterator.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="file_store_key_iterator.go" />
                      <item name="file_store_key_iterator.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/file_store_observer.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="file_store_observer.go" />
                      <item name="file_store_observer.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/float.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="float.go" />
                      <item name="float.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/int.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="int.go" />
                      <item name="int.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/iterator.gen.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="iterator.gen.go" />
                      <item name="iterator.gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/iterator.gen.go.tmpl" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="iterator.gen.go.tmpl" />
                      <item name="iterator.gen.go.tmpl" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/iterator.gen.go.tmpldata" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="iterator.gen.go.tmpldata" />
                      <item name="iterator.gen.go.tmpldata" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/iterator.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="iterator.go" />
                      <item name="iterator.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/mmap_unix.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="mmap_unix.go" />
                      <item name="mmap_unix.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/mmap_windows.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="mmap_windows.go" />
                      <item name="mmap_windows.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/pools.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="pools.go" />
                      <item name="pools.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/predicate.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="predicate.go" />
                      <item name="predicate.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/reader.gen.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="reader.gen.go" />
                      <item name="reader.gen.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/reader.gen.go.tmpl" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="reader.gen.go.tmpl" />
                      <item name="reader.gen.go.tmpl" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/reader.gen.go.tmpldata" />
                        </map>
                      </attributes>
                      <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="reader.gen.go.tmpldata" />
                      <item name="reader.gen.go.tmpldata" type="ab9368bb:PsiFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/reader.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="reader.go" />
                      <item name="reader.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/ring.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="ring.go" />
                      <item name="ring.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/scheduler.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="scheduler.go" />
                      <item name="scheduler.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/string.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="string.go" />
                      <item name="string.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/timestamp.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="timestamp.go" />
                      <item name="timestamp.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/tombstone.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="tombstone.go" />
                      <item name="tombstone.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/wal.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="wal.go" />
                      <item name="wal.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                    <presentation>
                      <attributes>
                        <map>
                          <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/tsm1/writer.go" />
                        </map>
                      </attributes>
                      <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="writer.go" />
                      <item name="writer.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                    </presentation>
                  </presentation>
                  <presentation>
                    <attributes>
                      <map>
                        <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine/engine.go" />
                      </map>
                    </attributes>
                    <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="true" text="engine.go" />
                    <item name="engine.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                  </presentation>
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/index" />
                    </map>
                  </attributes>
                  <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="index" />
                  <item name="dir{file:///home/a/github/influxdb/tsdb/index}" type="462c0819:PsiDirectoryNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/internal" />
                    </map>
                  </attributes>
                  <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="internal" />
                  <item name="dir{file:///home/a/github/influxdb/tsdb/internal}" type="462c0819:PsiDirectoryNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/config.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="config.go" />
                  <item name="config.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/cursor.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="cursor.go" />
                  <item name="cursor.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/engine.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="engine.go" />
                  <item name="engine.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/enginetags.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="enginetags.go" />
                  <item name="enginetags.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/epoch_tracker.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="epoch_tracker.go" />
                  <item name="epoch_tracker.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/field_validator.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="field_validator.go" />
                  <item name="field_validator.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/guard.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="guard.go" />
                  <item name="guard.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/index.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="index.go" />
                  <item name="index.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/meta.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="meta.go" />
                  <item name="meta.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/README.md" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/markdown.svg" iconPlugin="org.intellij.plugins.markdown" isLeaf="true" text="README.md" />
                  <item name="README.md" type="ab9368bb:PsiFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/series_cursor.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="series_cursor.go" />
                  <item name="series_cursor.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/series_file.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="series_file.go" />
                  <item name="series_file.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/series_index.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="series_index.go" />
                  <item name="series_index.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/series_partition.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="series_partition.go" />
                  <item name="series_partition.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/series_segment.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="series_segment.go" />
                  <item name="series_segment.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/series_set.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="series_set.go" />
                  <item name="series_set.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/shard.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="shard.go" />
                  <item name="shard.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
                <presentation>
                  <attributes>
                    <map>
                      <entry key="filePath" value="$PROJECT_DIR$/tsdb/store.go" />
                    </map>
                  </attributes>
                  <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="store.go" />
                  <item name="store.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
                </presentation>
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/ui" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="ui" />
                <item name="dir{file:///home/a/github/influxdb/ui}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/uuid" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="uuid" />
                <item name="dir{file:///home/a/github/influxdb/uuid}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/v1" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="v1" />
                <item name="dir{file:///home/a/github/influxdb/v1}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/vault" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="vault" />
                <item name="dir{file:///home/a/github/influxdb/vault}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/zap" />
                  </map>
                </attributes>
                <data iconPath="expui/nodes/folder.svg" iconPlugin="com.intellij" isLeaf="false" text="zap" />
                <item name="dir{file:///home/a/github/influxdb/zap}" type="462c0819:PsiDirectoryNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/.editorconfig" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/editorConfig.svg" iconPlugin="com.intellij" isLeaf="true" text=".editorconfig" />
                <item name=".editorconfig" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/.gitignore" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/ignored.svg" iconPlugin="com.intellij" isLeaf="true" text=".gitignore" />
                <item name=".gitignore" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/annotation.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="annotation.go" />
                <item name="annotation.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/api-compat.Jenkinsfile" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="api-compat.Jenkinsfile" />
                <item name="api-compat.Jenkinsfile" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/appveyor.yml" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/yaml.svg" iconPlugin="com.intellij" isLeaf="true" text="appveyor.yml" />
                <item name="appveyor.yml" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/auth.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="auth.go" />
                <item name="auth.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/authz.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="authz.go" />
                <item name="authz.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/backup.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="backup.go" />
                <item name="backup.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/bucket.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="bucket.go" />
                <item name="bucket.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/build.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="build.go" />
                <item name="build.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/CHANGELOG.md" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/markdown.svg" iconPlugin="org.intellij.plugins.markdown" isLeaf="true" text="CHANGELOG.md" />
                <item name="CHANGELOG.md" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/CHANGELOG_OLD.md" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/markdown.svg" iconPlugin="org.intellij.plugins.markdown" isLeaf="true" text="CHANGELOG_OLD.md" />
                <item name="CHANGELOG_OLD.md" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/check.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="check.go" />
                <item name="check.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/cliff.toml" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="cliff.toml" />
                <item name="cliff.toml" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/CONTRIBUTING.md" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/markdown.svg" iconPlugin="org.intellij.plugins.markdown" isLeaf="true" text="CONTRIBUTING.md" />
                <item name="CONTRIBUTING.md" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/credentials.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="credentials.go" />
                <item name="credentials.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/crud_log.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="crud_log.go" />
                <item name="crud_log.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/dashboard.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="dashboard.go" />
                <item name="dashboard.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/dbrp_mapping.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="dbrp_mapping.go" />
                <item name="dbrp_mapping.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/delete.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="delete.go" />
                <item name="delete.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/document.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="document.go" />
                <item name="document.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/duration.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="duration.go" />
                <item name="duration.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/env" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="env" />
                <item name="env" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/flags.yml" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/yaml.svg" iconPlugin="com.intellij" isLeaf="true" text="flags.yml" />
                <item name="flags.yml" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/FUZZ.md" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/markdown.svg" iconPlugin="org.intellij.plugins.markdown" isLeaf="true" text="FUZZ.md" />
                <item name="FUZZ.md" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/GNUmakefile" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/makefile.svg" iconPlugin="name.kropp.intellij.makefile" isLeaf="true" text="GNUmakefile" />
                <item name="GNUmakefile" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/go.mod" />
                  </map>
                </attributes>
                <data iconPath="icons/goMod.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="go.mod" />
                <item name="go.mod" type="620a8d5e:NestingTreeNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/influxd" />
                  </map>
                </attributes>
                <data iconPath="nodes/nodePlaceholder.svg" iconPlugin="com.intellij" isLeaf="true" text="influxd" />
                <item name="influxd" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/keyvalue_log.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="keyvalue_log.go" />
                <item name="keyvalue_log.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/label.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="label.go" />
                <item name="label.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/LICENSE" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="LICENSE" />
                <item name="LICENSE" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/lookup.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="lookup.go" />
                <item name="lookup.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/measurement.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="measurement.go" />
                <item name="measurement.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/measurement_schema.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="measurement_schema.go" />
                <item name="measurement_schema.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/measurement_schema_errors.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="measurement_schema_errors.go" />
                <item name="measurement_schema_errors.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/notebook.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="notebook.go" />
                <item name="notebook.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/notification.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="notification.go" />
                <item name="notification.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/notification_endpoint.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="notification_endpoint.go" />
                <item name="notification_endpoint.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/onboarding.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="onboarding.go" />
                <item name="onboarding.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/operation_log.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="operation_log.go" />
                <item name="operation_log.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/organization.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="organization.go" />
                <item name="organization.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/paging.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="paging.go" />
                <item name="paging.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/passwords.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="passwords.go" />
                <item name="passwords.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/preview.flux" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="preview.flux" />
                <item name="preview.flux" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/README.md" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/markdown.svg" iconPlugin="org.intellij.plugins.markdown" isLeaf="true" text="README.md" />
                <item name="README.md" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/remote_connection.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="remote_connection.go" />
                <item name="remote_connection.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/replication.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="replication.go" />
                <item name="replication.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/scraper.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="scraper.go" />
                <item name="scraper.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/secret.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="secret.go" />
                <item name="secret.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/SECURITY.md" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/markdown.svg" iconPlugin="org.intellij.plugins.markdown" isLeaf="true" text="SECURITY.md" />
                <item name="SECURITY.md" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/seed.flux" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="seed.flux" />
                <item name="seed.flux" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/semaphore.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="semaphore.go" />
                <item name="semaphore.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/session.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="session.go" />
                <item name="session.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/source.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="source.go" />
                <item name="source.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/status.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="status.go" />
                <item name="status.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/tag.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="tag.go" />
                <item name="tag.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/telegraf.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="telegraf.go" />
                <item name="telegraf.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/test.flux" />
                  </map>
                </attributes>
                <data iconPath="expui/fileTypes/unknown.svg" iconPlugin="com.intellij" isLeaf="true" text="test.flux" />
                <item name="test.flux" type="ab9368bb:PsiFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/token.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="token.go" />
                <item name="token.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/tools.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="true" text="tools.go" />
                <item name="tools.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/usage.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="usage.go" />
                <item name="usage.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/user.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="user.go" />
                <item name="user.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/user_resource_mapping.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="user_resource_mapping.go" />
                <item name="user_resource_mapping.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/variable.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="variable.go" />
                <item name="variable.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
              <presentation>
                <attributes>
                  <map>
                    <entry key="filePath" value="$PROJECT_DIR$/write.go" />
                  </map>
                </attributes>
                <data iconPath="icons/expui/go.svg" iconPlugin="org.jetbrains.plugins.go" isLeaf="false" text="write.go" />
                <item name="write.go" type="e9541c9c:GoTreeStructureProvider$GoFileNode" />
              </presentation>
            </presentation>
            <presentation>
              <attributes />
              <data iconPath="expui/nodes/library.svg" iconPlugin="com.intellij" isLeaf="false" text="External Libraries" />
              <item name="External Libraries" type="cb654da1:ExternalLibrariesNode" />
            </presentation>
            <presentation>
              <attributes />
              <data iconPath="expui/fileTypes/scratches.svg" iconPlugin="com.intellij" isLeaf="false" text="Scratches and Consoles" />
              <item name="Scratches and Consoles" type="b85a3e1f:ScratchTreeStructureProvider$MyProjectNode" />
            </presentation>
          </presentation>
        </subPane>
      </pane>
      <pane id="Scope" />
    </panes>
  </component>
  <component name="RunConfigurationStartHistory">
    <history>
      <element setting="Go Build.influxd" />
    </history>
  </component>
  <component name="TerminalArrangementManager">
    <option name="myTabStates">
      <TerminalTabState tabName="Local" currentWorkingDirectory="$PROJECT_DIR$/bin/linux" commandHistoryFileName="influxdb-history1">
        <shellCommand>
          <arg value="/bin/bash" />
          <arg value="-i" />
        </shellCommand>
      </TerminalTabState>
    </option>
  </component>
  <component name="ToolWindowManager">
    <layoutV2>
      <window_info id="Pull Requests" show_stripe_button="false" />
      <window_info content_ui="combo" id="Project" order="0" sideWeight="0.3776178" weight="0.2795139" />
      <window_info id="Commit" order="1" weight="0.25" />
      <window_info active="true" id="Bookmarks" order="2" sideWeight="0.62238216" side_tool="true" visible="true" weight="0.31145835" />
      <window_info id="Structure" order="3" show_stripe_button="false" side_tool="true" weight="0.25" />
      <window_info anchor="bottom" id="Terminal" order="0" show_stripe_button="false" weight="0.16768293" />
      <window_info anchor="bottom" content_ui="combo" id="Notifications" order="1" side_tool="true" weight="0.25" />
      <window_info anchor="bottom" id="Version Control" order="2" show_stripe_button="false" weight="0.425" />
      <window_info anchor="bottom" id="Problems" order="3" />
      <window_info anchor="bottom" id="Problems View" order="4" show_stripe_button="false" />
      <window_info anchor="bottom" id="Services" order="5" show_stripe_button="false" />
      <window_info anchor="bottom" id="Run" order="6" show_stripe_button="false" weight="0.44297236" />
      <window_info anchor="bottom" id="Find" order="7" show_stripe_button="false" weight="0.2554878" />
      <window_info anchor="bottom" id="Debug" order="8" show_stripe_button="false" weight="0.4335366" />
      <window_info anchor="right" id="AIAssistant" order="0" show_stripe_button="false" weight="0.25" />
      <window_info anchor="right" id="Database" order="1" weight="0.25" />
      <window_info anchor="right" id="Gradle" order="2" weight="0.25" />
      <window_info anchor="right" id="Maven" order="3" weight="0.25" />
      <unified_weights bottom="0.2554878" left="0.31145835" />
    </layoutV2>
    <layout-to-restore>
      <window_info id="Pull Requests" show_stripe_button="false" />
      <window_info active="true" content_ui="combo" id="Project" order="0" sideWeight="0.3776178" visible="true" weight="0.35173613" />
      <window_info id="Commit" order="1" weight="0.25" />
      <window_info active="true" id="Bookmarks" order="2" sideWeight="0.62238216" side_tool="true" visible="true" weight="0.35173613" />
      <window_info id="Structure" order="3" show_stripe_button="false" side_tool="true" weight="0.25" />
      <window_info anchor="bottom" id="Terminal" order="0" show_stripe_button="false" weight="0.16768293" />
      <window_info anchor="bottom" content_ui="combo" id="Notifications" order="1" side_tool="true" weight="0.25" />
      <window_info anchor="bottom" id="Version Control" order="2" show_stripe_button="false" weight="0.425" />
      <window_info anchor="bottom" id="Problems" order="3" />
      <window_info anchor="bottom" id="Problems View" order="4" show_stripe_button="false" />
      <window_info anchor="bottom" id="Services" order="5" show_stripe_button="false" />
      <window_info anchor="bottom" id="Run" order="6" show_stripe_button="false" weight="0.44297236" />
      <window_info anchor="bottom" id="Find" order="7" show_stripe_button="false" weight="0.2554878" />
      <window_info anchor="bottom" id="Debug" order="8" weight="0.2554878" />
      <window_info anchor="right" id="AIAssistant" order="0" show_stripe_button="false" weight="0.25" />
      <window_info anchor="right" id="Database" order="1" weight="0.25" />
      <window_info anchor="right" id="Gradle" order="2" weight="0.25" />
      <window_info anchor="right" id="Maven" order="3" weight="0.25" />
      <unified_weights bottom="0.2554878" left="0.35173613" />
    </layout-to-restore>
    <recentWindows>
      <value>Bookmarks</value>
      <value>Find</value>
      <value>Project</value>
      <value>Debug</value>
      <value>Version Control</value>
      <value>Terminal</value>
      <value>Run</value>
    </recentWindows>
  </component>
  <component name="WindowStateProjectService">
    <state x="539" y="221" width="410" height="589" key="#Build_Tags" timestamp="1743138662251">
      <screen x="48" y="32" width="1392" height="868" />
    </state>
    <state x="539" y="221" width="410" height="589" key="#Build_Tags/48.32.1392.868@48.32.1392.868" timestamp="1743138662251" />
    <state x="339" y="125" width="800" height="712" key="#com.intellij.execution.impl.EditConfigurationsDialog" timestamp="1743140760003">
      <screen x="48" y="32" width="1392" height="868" />
    </state>
    <state x="339" y="125" width="800" height="712" key="#com.intellij.execution.impl.EditConfigurationsDialog/48.32.1392.868@48.32.1392.868" timestamp="1743140760003" />
    <state x="316" y="232" width="904" height="562" key="AttachToProcessDialog" timestamp="1743141924892">
      <screen x="48" y="32" width="1392" height="868" />
    </state>
    <state x="316" y="232" width="904" height="562" key="AttachToProcessDialog/48.32.1392.868@48.32.1392.868" timestamp="1743141924892" />
    <state width="627" height="147" key="DebuggerActiveHint" timestamp="1743470242387">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state width="627" height="147" key="DebuggerActiveHint/0.32.1440.820@0.32.1440.820" timestamp="1743470242387" />
    <state x="527" y="225" width="424" height="495" key="FileChooserDialogImpl" timestamp="1743139751565">
      <screen x="48" y="32" width="1392" height="868" />
    </state>
    <state x="527" y="225" width="424" height="495" key="FileChooserDialogImpl/48.32.1392.868@48.32.1392.868" timestamp="1743139751565" />
    <state width="1374" height="323" key="GridCell.Tab.0.bottom" timestamp="1743595599102">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state width="1374" height="323" key="GridCell.Tab.0.bottom/0.32.1440.820@0.32.1440.820" timestamp="1743595599102" />
    <state width="1326" height="301" key="GridCell.Tab.0.bottom/48.32.1392.868@48.32.1392.868" timestamp="1743145425990" />
    <state width="1374" height="323" key="GridCell.Tab.0.center" timestamp="1743595599102">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state width="1374" height="323" key="GridCell.Tab.0.center/0.32.1440.820@0.32.1440.820" timestamp="1743595599102" />
    <state width="1326" height="301" key="GridCell.Tab.0.center/48.32.1392.868@48.32.1392.868" timestamp="1743145425990" />
    <state width="1374" height="323" key="GridCell.Tab.0.left" timestamp="1743595599102">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state width="1374" height="323" key="GridCell.Tab.0.left/0.32.1440.820@0.32.1440.820" timestamp="1743595599102" />
    <state width="1326" height="301" key="GridCell.Tab.0.left/48.32.1392.868@48.32.1392.868" timestamp="1743145425990" />
    <state width="1374" height="323" key="GridCell.Tab.0.right" timestamp="1743595599102">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state width="1374" height="323" key="GridCell.Tab.0.right/0.32.1440.820@0.32.1440.820" timestamp="1743595599102" />
    <state width="1326" height="301" key="GridCell.Tab.0.right/48.32.1392.868@48.32.1392.868" timestamp="1743145425990" />
    <state width="1374" height="323" key="GridCell.Tab.1.bottom" timestamp="1743595599102">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state width="1374" height="323" key="GridCell.Tab.1.bottom/0.32.1440.820@0.32.1440.820" timestamp="1743595599102" />
    <state width="1326" height="301" key="GridCell.Tab.1.bottom/48.32.1392.868@48.32.1392.868" timestamp="1743145425362" />
    <state width="1374" height="323" key="GridCell.Tab.1.center" timestamp="1743595599102">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state width="1374" height="323" key="GridCell.Tab.1.center/0.32.1440.820@0.32.1440.820" timestamp="1743595599102" />
    <state width="1326" height="301" key="GridCell.Tab.1.center/48.32.1392.868@48.32.1392.868" timestamp="1743145425361" />
    <state width="1374" height="323" key="GridCell.Tab.1.left" timestamp="1743595599102">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state width="1374" height="323" key="GridCell.Tab.1.left/0.32.1440.820@0.32.1440.820" timestamp="1743595599102" />
    <state width="1326" height="301" key="GridCell.Tab.1.left/48.32.1392.868@48.32.1392.868" timestamp="1743145425361" />
    <state width="1374" height="323" key="GridCell.Tab.1.right" timestamp="1743595599102">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state width="1374" height="323" key="GridCell.Tab.1.right/0.32.1440.820@0.32.1440.820" timestamp="1743595599102" />
    <state width="1326" height="301" key="GridCell.Tab.1.right/48.32.1392.868@48.32.1392.868" timestamp="1743145425361" />
    <state x="836" y="732" width="583" height="133" key="ProcessPopupWindow" timestamp="1743139938773">
      <screen x="48" y="32" width="1392" height="868" />
    </state>
    <state x="836" y="732" width="583" height="133" key="ProcessPopupWindow/48.32.1392.868@48.32.1392.868" timestamp="1743139938773" />
    <state x="549" y="192" width="380" height="554" key="RollbackChangesDialog" timestamp="1743140002931">
      <screen x="48" y="32" width="1392" height="868" />
    </state>
    <state x="549" y="192" width="380" height="554" key="RollbackChangesDialog/48.32.1392.868@48.32.1392.868" timestamp="1743140002931" />
    <state x="248" y="101" width="992" height="737" key="SettingsEditor" timestamp="1743140224253">
      <screen x="48" y="32" width="1392" height="868" />
    </state>
    <state x="248" y="101" width="992" height="737" key="SettingsEditor/48.32.1392.868@48.32.1392.868" timestamp="1743140224253" />
    <state width="720" height="410" key="XDebugger.FullValuePopup" timestamp="1743235208316">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state width="720" height="410" key="XDebugger.FullValuePopup/0.32.1440.820@0.32.1440.820" timestamp="1743235208316" />
    <state x="489" y="203" width="451" height="484" key="com.goide.refactor.extractInterface.GoExtractInterfaceDialog" timestamp="1743398348810">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state x="489" y="203" width="451" height="484" key="com.goide.refactor.extractInterface.GoExtractInterfaceDialog/0.32.1440.820@0.32.1440.820" timestamp="1743398348810" />
    <state x="250" y="155" width="930" height="580" key="com.intellij.xdebugger.impl.breakpoints.ui.BreakpointsDialogFactory$2" timestamp="1743583977786">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state x="250" y="155" width="930" height="580" key="com.intellij.xdebugger.impl.breakpoints.ui.BreakpointsDialogFactory$2/0.32.1440.820@0.32.1440.820" timestamp="1743583977786" />
    <state x="476" y="103" width="674" height="749" key="find.popup" timestamp="1743479769181">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state x="476" y="103" width="674" height="749" key="find.popup/0.32.1440.820@0.32.1440.820" timestamp="1743479769181" />
    <state x="508" y="108" width="651" height="792" key="find.popup/48.32.1392.868@48.32.1392.868" timestamp="1743141711957" />
    <state x="371" y="201" width="720" height="664" key="search.everywhere.popup" timestamp="1743591845812">
      <screen x="0" y="32" width="1440" height="820" />
    </state>
    <state x="371" y="201" width="720" height="664" key="search.everywhere.popup/0.32.1440.820@0.32.1440.820" timestamp="1743591845812" />
    <state x="407" y="211" width="696" height="702" key="search.everywhere.popup/48.32.1392.868@48.32.1392.868" timestamp="1743145559189" />
  </component>
  <component name="editorHistoryManager">
    <entry file="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/execute/transformation.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="120">
          <caret line="17" column="4" selection-start-line="17" selection-start-column="4" selection-end-line="17" selection-end-column="4" />
        </state>
      </provider>
    </entry>
    <entry file="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/csv/result.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="-588">
          <caret line="905" column="58" lean-forward="true" selection-start-line="905" selection-start-column="58" selection-end-line="905" selection-end-column="58" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/encoding.gen.go.tmpl">
      <provider selected="true" editor-type-id="text-editor" />
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/encoding.gen.go.tmpldata">
      <provider selected="true" editor-type-id="text-editor" />
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.gen.go.tmpl">
      <provider selected="true" editor-type-id="text-editor" />
    </entry>
    <entry file="file://$PROJECT_DIR$/v1/coordinator/shard_mapper.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="484">
          <caret line="206" column="33" selection-start-line="206" selection-start-column="33" selection-end-line="206" selection-end-column="33" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.gen.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="188">
          <caret line="15" column="24" lean-forward="true" selection-start-line="15" selection-start-column="24" selection-end-line="15" selection-end-column="24" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/v1/services/storage/store.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="2976">
          <caret line="186" selection-start-line="186" selection-end-line="186" />
        </state>
      </provider>
    </entry>
    <entry file="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/array/array.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="544">
          <caret line="34" column="5" selection-start-line="34" selection-start-column="5" selection-end-line="34" selection-end-column="5" />
        </state>
      </provider>
    </entry>
    <entry file="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/arrow/int.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="112">
          <caret line="7" column="5" selection-start-line="7" selection-start-column="5" selection-end-line="7" selection-end-column="5" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/series_file.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="576">
          <caret line="36" column="8" selection-start-line="36" selection-start-column="8" selection-end-line="36" selection-end-column="8" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/cmd/influxd/inspect/build_tsi/build_tsi.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="6448">
          <caret line="403" column="13" selection-start-line="403" selection-start-column="13" selection-end-line="403" selection-end-column="13" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/index/tsi1/testdata/index-file-index/0/MANIFEST">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="528">
          <caret line="33" column="17" selection-start-line="33" selection-start-column="17" selection-end-line="33" selection-end-column="17" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/index/tsi1/index_file.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="8448">
          <caret line="528" column="53" selection-start-line="528" selection-start-column="53" selection-end-line="528" selection-end-column="53" />
        </state>
      </provider>
    </entry>
    <entry file="file://$USER_HOME$/gopath/pkg/mod/golang.org/x/sync@v0.4.0/errgroup/errgroup.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="1040">
          <caret line="65" column="16" selection-start-line="65" selection-start-column="16" selection-end-line="65" selection-end-column="16" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/cmd/influxd/launcher/cmd.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="8816">
          <caret line="551" column="56" selection-start-line="551" selection-start-column="56" selection-end-line="551" selection-end-column="56" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/config.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="576">
          <caret line="36" column="4" selection-start-line="36" selection-start-column="4" selection-end-line="36" selection-end-column="4" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/v1/coordinator/points_writer.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="7152">
          <caret line="447" selection-start-line="447" selection-end-line="447" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/wal.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="7504">
          <caret line="469" selection-start-line="469" selection-end-line="469" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/store.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="3616">
          <caret line="226" column="29" lean-forward="true" selection-start-line="226" selection-start-column="29" selection-end-line="226" selection-end-column="29" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/writer.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="4048">
          <caret line="253" column="5" selection-start-line="253" selection-start-column="5" selection-end-line="253" selection-end-column="5" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/series_partition.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="244">
          <caret line="198" column="40" selection-start-line="198" selection-start-column="40" selection-end-line="198" selection-end-column="40" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/models/points.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="244">
          <caret line="1975" column="5" selection-start-line="1975" selection-start-column="5" selection-end-line="1975" selection-end-column="5" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="53" column="4" selection-start-line="53" selection-start-column="4" selection-end-line="53" selection-end-column="4" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/index.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="41" column="4" selection-start-line="41" selection-start-column="4" selection-end-line="41" selection-end-column="4" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/index/tsi1/log_file.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="16">
          <caret line="519" column="24" selection-start-line="519" selection-start-column="24" selection-end-line="519" selection-end-column="24" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/index/tsi1/partition.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="92">
          <caret line="709" column="28" selection-start-line="709" selection-start-column="28" selection-end-line="709" selection-end-column="28" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/index/tsi1/index.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="268">
          <caret line="645" column="24" lean-forward="true" selection-start-line="645" selection-start-column="24" selection-end-line="645" selection-end-column="24" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/shard.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="172">
          <caret line="656" column="24" lean-forward="true" selection-start-line="656" selection-start-column="24" selection-end-line="656" selection-end-column="24" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/storage/reads/array_cursor.gen.go.tmpl">
      <provider selected="true" editor-type-id="text-editor" />
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/cache.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="244">
          <caret line="160" column="5" selection-start-line="160" selection-start-column="5" selection-end-line="160" selection-end-column="5" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/storage/flux/table.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="252">
          <caret line="73" column="16" lean-forward="true" selection-start-line="73" selection-start-column="16" selection-end-line="73" selection-end-column="16" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/storage/flux/table.gen.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="38" column="5" selection-start-line="38" selection-start-column="5" selection-end-line="38" selection-end-column="5" />
        </state>
      </provider>
    </entry>
    <entry file="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/execute/executor.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="244">
          <caret line="76" column="27" selection-start-line="76" selection-start-column="27" selection-end-line="76" selection-end-column="27" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/storage/reads/store.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="166">
          <caret line="17" column="4" selection-start-line="17" selection-start-column="4" selection-end-line="17" selection-end-column="4" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/storage/reads/series_cursor.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="22" column="19" selection-start-line="22" selection-start-column="19" selection-end-line="22" selection-end-column="19" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/storage/reads/array_cursor.gen.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="252">
          <caret line="269" column="18" selection-start-line="269" selection-start-column="18" selection-end-line="269" selection-end-column="18" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor.gen.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="284">
          <caret line="48" selection-start-line="48" selection-end-line="48" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/storage/reads/resultset.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="117">
          <caret line="60" selection-start-line="60" selection-end-line="60" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/storage/reads/array_cursor.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="117" column="36" selection-start-line="117" selection-start-column="36" selection-end-line="117" selection-end-column="36" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/cursors/cursor.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="70" column="4" selection-start-line="70" selection-start-column="4" selection-end-line="70" selection-end-column="4" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/query/stdlib/influxdata/influxdb/source.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="69" column="27" lean-forward="true" selection-start-line="69" selection-start-column="27" selection-end-line="69" selection-end-column="27" />
        </state>
      </provider>
    </entry>
    <entry file="file://$USER_HOME$/gopath/pkg/mod/github.com/influxdata/flux@v0.194.4/execute/result.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="636">
          <caret line="56" column="1" lean-forward="true" selection-start-line="56" selection-start-column="1" selection-end-line="56" selection-end-column="1" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor_iterator.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="278">
          <caret line="64" column="53" selection-start-line="64" selection-start-column="53" selection-end-line="64" selection-end-column="53" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/array_cursor_iterator.gen.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="284">
          <caret line="20" column="50" lean-forward="true" selection-start-line="20" selection-start-column="50" selection-end-line="20" selection-end-column="50" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/storage/flux/reader.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="211" selection-start-line="211" selection-end-line="211" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/DESIGN.md">
      <provider selected="true" editor-type-id="split-provider[text-editor;markdown-preview-editor]">
        <state split_layout="SHOW_EDITOR" is_vertical_split="false">
          <first_editor relative-caret-position="-144" />
          <second_editor />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/reader.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="221" column="5" selection-start-line="221" selection-start-column="5" selection-end-line="221" selection-end-column="5" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/compact.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="860" selection-start-line="860" selection-end-line="860" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/file_store.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="508">
          <caret line="958" column="10" selection-start-line="958" selection-start-column="10" selection-end-line="958" selection-end-column="10" />
        </state>
      </provider>
    </entry>
    <entry file="file://$PROJECT_DIR$/tsdb/engine/tsm1/engine.go">
      <provider selected="true" editor-type-id="text-editor">
        <state relative-caret-position="236">
          <caret line="469" selection-start-line="469" selection-end-line="469" />
        </state>
      </provider>
    </entry>
  </component>
</project>
```

```text
DtH7AsjLoas70Osrzbo4TGkdv8OKX9WesR8iL-C3uGIjHsFlOQKbmxX4TGhNEdvAlLNlMieGNSoswkvBF5AJ_w==
```
