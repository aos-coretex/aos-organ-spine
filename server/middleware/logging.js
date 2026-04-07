/**
 * Structured JSON logging middleware.
 * Logs one JSON object per line to stdout for every HTTP request.
 */

export function loggingMiddleware(req, res, next) {
  const start = Date.now();
  res.on('finish', () => {
    const entry = {
      timestamp: new Date().toISOString(),
      method: req.method,
      path: req.originalUrl || req.path,
      status: res.statusCode,
      duration_ms: Date.now() - start,
    };
    process.stdout.write(JSON.stringify(entry) + '\n');
  });
  next();
}
