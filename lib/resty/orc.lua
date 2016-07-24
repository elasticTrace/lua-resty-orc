local _M = {}
local _M['tlc_mgr'] = {}
local _M['system'] = {}
local _M['cluster']

local resty_consul = require('resty.consul') -- https://github.com/hamishforbes/lua-resty-consul
local tlc_mgr = require('resty.tlc.manager') -- https://github.com/hamishforbes/lua-resty-tlc
local lyaml = require('lyaml') -- https://github.com/gvvaughan/lyaml
local lfs = require('luafilesystem') -- https://github.com/keplerproject/luafilesystem

function _M.new()
    local that = {}
    setmetatable(that, _M)
    return that
end -- new

function _M.init_by_lua(consul_opts)
    config = load_settings(false)

    -- setup settings system
    if settings_opts == nil then
        settings_opts = {size = config["settings"]["tlc"]["size"], config["settings"]["tlc"]["dict"]}
        if settings_opts['size'] == nil or type(settings_opts['size']) ~= number then
            settings_opts['size'] = 500
        end -- if

        if settings_opts['dict'] == nil or type(settings_opts['dict']) ~= string then
            settings_opts['dict'] = "orc_tlc_consul"
        end -- if
    end

    local config_tlc = tlc.new("_orc_config", settings_opts)
    if config_tlc == nil or type(config_tlc) == "error" then
    end


    -- setup consul cacheing control
    if consul_opts == nil then
        consul_opts = {size = config["consul"]["tlc"]["size"], config["consul"]["tlc"]["dict"]}
    else
        if consul_opts['size'] == nil or type(consul_opts['size']) ~= number then
            consul_opts['size'] = 500
        end -- if

        if consul_opts['dict'] == nil or type(consul_opts['dict']) ~= string then
            consul_opts['dict'] = "orc_tlc_consul"
        end -- if
    end --if

    local consul_tlc = tlc_mgr.new("_orc_consul", consul_opts)
    if consul_tlc == nil or type(consul_tlc) == "error" then
    end

    load_settings() --run load settings again but this time save it

    tlc_mgr.new("_orc_haelth", {size = _default(config['cluster']['max_node_count'], 1024), dict = "orc_tlc_health"})
    tlc_mgr.new("_orc_haelth_report", {size = _default(config['cluster']['max_node_count'], 1024), dict = "orc_tlc_health_report"})

end -- init_by_lua

function _M.init_worker_by_lua(self)
    self.health = tlc_mgr.get("_orc_haelth")
    self.config = tlc_mgr.get("_orc_config")
    self.consul = resty_consul:new({
        host = config.get("consul")['host'],
        port = config.get("consul")['port']
    })


    -- setup job to keep clustering up to date
    if ngx.worker.id() == 0 then
        local ok, err = self:cluster_join()-- join the cluster

    end -- if
end -- init_worker_by_lua

local function _default(val, err_return)
    if val == nil then
        return err_return
    end -- if
end _default

-- handles requests that come though the upstream request because
-- we had a push come in
function _M.nchan_publish_upstream_proxy()
    -- read the channel that this message is for

    -- check if this message is suppose to be localhost

    -- check if this is reserved channel name

    -- check if the message is broadcast to all nodes in cluster

    -- check if message is fixed string match

    -- check if message is wildcard match

end -- function

-- load my own system meta data
function system_load_meta()
    local meta = {}
    meta['nginx']["version"] = ngx.config.nginx_version
    meta['nginx']['worker_count'] = ngx.worker_count()
    meta['lua']['version'] = ngx.config.ngx_lua_version
    meta['linux']['hostname'] = string.gsub(io.popen ("/bin/hostname"):read("*a") or "", "\n$", "")

    return meta
end

function settings_load(update)
    local config_path = os.getenv("ORC_CONFIG")

    if type(config_path) ~= "string" then
        return nil, "ORC_CONFIG was empty or not a string"
    end -- if

    local file_exists = os.rename(config_path, config_path)
    local config = nil
    if file_exists == true then
        config = lyaml.load(config_path)
        if type(config) ~= "table" then
            return nil, "ORC_CONFIG failed to parse"
        end -- if

        -- dont push the settings up useful for when we are starting up
        if update == false then
            return config
        end -- if

        local settings_tlc = tlc.get("orc_settings_tlc")
        for dex, dat in paris(config) do -- load all the settings into our local
            settings_tlc:set(dex, dat)
        end -- for

    else
        return nil, "ORC_CONFIG: \""..config_path.."\" did not exist"
    end
    return true
end -- settings_load

function _M.cluster_join(cluster_name)
    -- connect to consul

    local fqdn = config.get('system')['fqdn']
    --local config = tlc.get("_orc_config")
    if cluster_name == nil then
        cluster_name = config.get('cluster')['name']
        if cluster_name == nil then
            return nil, "Mising cluster.name in config"
        end -- if
    end -- if

    if fqdn == nil then
        return nil, "Missing system.fqdn in config"
    end --if

    local ok, err = self.consul.get("/kv/_orc/"..cluster_name.."/members/"..fqdn)
    if ok == nil then
        local ok, err = self.consul.put("/kv/_orc/"..cluster_name.."/members/"..fqdn, system_load_meta())
        if err ~= nil then
            -- we will need to find a way to handle making sure all other workers know this
            return cluster_name
        end -- if
    end -- if

    return nil, "error joining cluster"

end -- cluster_join

function _M.cluster_leave()
    -- todo: we will want to have a version that calls out to all the nodes that are on
        -- and lets them know that we are going off line

    --local config = tlc.get("_orc_config")
    local ok, err = self.consul.delete("/kv/_orc/"..config.get("cluster")['name'].."/members/"..config.get('system')['fqdn'], nil, true)

    if ok ~= true then
        return false, status, body, headers
    end -- if

    return true
end -- cluster_leave

function _M.cluster_get_hosts()
    local ok, err = self.consul.get("/kv/_orc/"..config.get("cluster")['name'].."/members/", "keys")
end -- cluster_get_hosts

function _M.cluster_cache_host()
end --cluster_cache_host

function _M.cluster_ping_host()
end --cluster_ping_host

function _M.cluster_queue_healthcheck()

    local hosts = self:cluster_get_hosts() -- get a list of servers in the cluster
    if hosts[config.get('system')['fqdn']] == nil then -- make sure we are marked as in the cluster
        self:cluster_join(self['cluster']['name']) -- rejoin the cluster because we are missing
    end

    hosts[config.get('system')['fqdn']] = nil -- remove our selves from the list
    for dex, dat in hosts do -- set up jobs to check check all the nodes in the cluster
        ngx.timer.at(0, self:cluster_do_healthcheck, self, dex, dat['cluster']['cluster_port'])
    end -- for
end -- cluster_healthcheck

function _M.cluster_do_healthcheck(premature self, host, port)
    if premature then
        return
    end -- if

    local httpc = http:new()
    local res, err = httpc:request_uri("http://"..host..":"..port.."/healthcheck", {})
    if err ~= nil then
        self.health:set(host, nil)
        self.health_report:set(host, {}) -- full report about it being down
        ngx.log(ngx.ALERT, host..":"..port.." is down")
        return nil, false
    end

    self.health:set(host, true)
    self.health_report:set(host, {}) -- full report on it being up
    ngx.log(ngx.ALERT, host..":"..port.." is up")
    return true
end


return _M
