import { randomUUID } from "crypto";
import type { Request, Response, NextFunction } from "express";

export interface RequestWithId extends Request {
  requestId?: string;
}

export function requestIdMiddleware(
  req: RequestWithId,
  res: Response,
  next: NextFunction,
): void {
  const id = (req.headers["x-request-id"] as string) || randomUUID();
  req.requestId = id;
  res.setHeader("x-request-id", id);
  next();
}
