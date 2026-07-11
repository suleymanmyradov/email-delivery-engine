type LogLevel = "debug" | "info" | "warn" | "error";

interface LogFields {
  [key: string]: unknown;
}

function emit(level: LogLevel, msg: string, fields?: LogFields): void {
  const line = JSON.stringify({
    level,
    msg,
    timestamp: new Date().toISOString(),
    ...fields,
  });
  const stream = level === "error" ? process.stderr : process.stdout;
  stream.write(line + "\n");
}

export const log = {
  debug: (msg: string, fields?: LogFields) => emit("debug", msg, fields),
  info: (msg: string, fields?: LogFields) => emit("info", msg, fields),
  warn: (msg: string, fields?: LogFields) => emit("warn", msg, fields),
  error: (msg: string, fields?: LogFields) => emit("error", msg, fields),
};
