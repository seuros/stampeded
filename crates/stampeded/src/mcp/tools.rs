//! One file per tool, each with its `mount`.

use super::StampededServer;

mcp_host::auto_tools!(StampededServer, "src/mcp/tools");
