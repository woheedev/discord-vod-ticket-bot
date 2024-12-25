import sqlite3 from "sqlite3";
import { open } from "sqlite";
import path from "path";

let db;
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // 1 second

const DB_PATH = path.resolve(__dirname, "../../tracker-bot/guild_data.db");

async function openDbReadOnly() {
  return open({
    filename: DB_PATH,
    driver: sqlite3.Database,
    mode: sqlite3.OPEN_READONLY, // Read-only mode
  });
}

export async function initializeDb() {
  let retries = 0;
  while (retries < MAX_RETRIES) {
    try {
      db = await openDbReadOnly();
      // Set pragmas for read-only optimization
      await db.run("PRAGMA query_only = ON");
      await db.run("PRAGMA read_uncommitted = ON");
      break;
    } catch (error) {
      retries++;
      if (retries === MAX_RETRIES) {
        throw new Error(
          `Failed to connect to database after ${MAX_RETRIES} attempts: ${error.message}`
        );
      }
      await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY));
    }
  }
}

export async function getIngameName(userId) {
  try {
    const result = await db.get(
      "SELECT ingame_name FROM ingame_names WHERE user_id = ?",
      [userId]
    );
    return result?.ingame_name || null;
  } catch (error) {
    if (error.code === "SQLITE_BUSY" || error.code === "SQLITE_LOCKED") {
      // Log but don't throw - let the application continue
      console.error("Database busy/locked:", error);
      return null;
    }
    throw error;
  }
}
