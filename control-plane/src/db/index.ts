import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import { env } from "../env";
import * as schema from "./schema";

export const pool = new Pool({ connectionString: env.DATABASE_URL });
// casing: 'snake_case' makes the query builder emit snake_case column names,
// matching the migration DDL and the raw SQL used elsewhere.
export const db = drizzle(pool, { schema, casing: "snake_case" });
