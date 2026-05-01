local pathlib = require("path")
local strings = require("strings")
local yaml = require("encoding/yaml")
local utils = require("sghub/catalogexport/internal")

local SGHUB_TABLES_BASE = "_hub_tables/"

-- check if the hub entry is a table spec under _hub_tables/
local function is_table_obj(entry, tables_base)
    if entry.path_type ~= "object" then
        return false
    end
    local path = entry.path
    if strings.has_prefix(path, tables_base) then
        -- remove _hub_tables/ from path
        path = entry.path:sub(#tables_base, #path)
    end
    return not pathlib.is_hidden(path) and strings.has_suffix(path, ".yaml")
end

-- list all YAML files under _hub_tables/*
local function list_table_descriptor_entries(client, repo_id, commit_id)
    local table_entries = {}
    local page_size = 30
    local pager = utils.sghub_object_pager(client, repo_id, commit_id, "", SGHUB_TABLES_BASE,"", page_size)
    for entries in pager do
        for _, entry in ipairs(entries) do
            if is_table_obj(entry, SGHUB_TABLES_BASE) then
                table.insert(table_entries, {
                    physical_address = entry.physical_address,
                    path = entry.path
                })
            end
        end
    end
    return table_entries
end

-- table as parsed YAML object
local function get_table_descriptor(client, repo_id, commit_id, logical_path)
    local code, content = client.get_object(repo_id, commit_id, logical_path)
    if code ~= 200 then
        error("could not fetch data file: HTTP " .. tostring(code) .. " path: " .. logical_path)
    end
    local descriptor = yaml.unmarshal(content)
    descriptor.partition_columns = descriptor.partition_columns or {}
    return descriptor
end

return {
    list_table_descriptor_entries = list_table_descriptor_entries,
    get_table_descriptor = get_table_descriptor,
}