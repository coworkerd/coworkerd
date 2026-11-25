import { SqlDatabase as SharedSqlDatabase } from "./sql-database.js";

export class SqlDatabase extends SharedSqlDatabase {
  static wallDeadline = true;
}
