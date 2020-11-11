const express = require("express");
const { postgraphile, makePluginHook } = require("postgraphile");
const PgSimplifyInflectorPlugin = require("@graphile-contrib/pg-simplify-inflector");
const http = require("http");
const morgan = require("morgan");
const compression = require("compression");
const helmet = require("helmet");
const bodyParser = require("body-parser");
const cors = require("cors");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(
  morgan(":method :url :status :res[content-length] - :response-time ms")
);
app.use(helmet());

const BODY_SIZE_LIMIT = "50MB";
app.use(compression({ filter: shouldCompress }));
app.use(
  bodyParser.json({
    limit: BODY_SIZE_LIMIT,
  })
);

function shouldCompress(req, res) {
  if (req.headers["x-no-compression"]) {
    // don't compress responses with this request header
    return false;
  }

  // fallback to standard filter function
  return compression.filter(req, res);
}

let dbSchema = "api"
if (process.env.POSTGRAPHILE_DB_SCHEMA) {
  dbSchema = process.env.POSTGRAPHILE_DB_SCHEMA
}


app.use(
  postgraphile(process.env.POSTGRAPHILE_DATABASE_URL, dbSchema, {
    watchPg: true,
    ownerConnectionString: process.env.ADMIN_DATABASE_URL,
    graphiql: true,
    defaultPaginationCap: -1,
    graphqlDepthLimit: 6,
    enhanceGraphiql: true,
    appendPlugins: [PgSimplifyInflectorPlugin],
    ignoreRBAC: false,
    statement_timeout: "3000",
    dynamicJson: true,
    // simpleCollections: "both",
    bodySizeLimit: BODY_SIZE_LIMIT,
  })
);

// start http server
const httpServer = http.createServer(app);

httpServer.listen(process.env.PORT, function () {
  console.log(
    "App listenting at localhost:" +
      process.env.PORT +
      "visit /graphiql endpoint"
  );
});
