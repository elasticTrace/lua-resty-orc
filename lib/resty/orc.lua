local _M = {}
local _M['tlc_mgr'] = {}
local _M['system'] = {}
local _M['cluster'] = {}
local _M['final'] = {channel_dict = "_orc_channel"}

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
        if err ~= nil then
            -- some how we need to end this all
        end
    end -- if
end -- init_worker_by_lua

-- keep track how many are connected to each channel
function _M.nchan_rewrite_by_lua(self, split)
    local channel_dict = ngx.shared[self.final['channel_dict']]
    local channels = self.split(, split)
    local ctx = ngx.ctx

    ctx._orc_channels = {}

    for dex,dat in channels do
        local ok, err = channel_dict.incr(dat, 1)
        if ok == nil and err == "not found" then
            local ok, err = channel_dict:add(dat, 1)
            if ok == false and err == "exists" then
                local ok, err = channel_dict:incr(dat, 1)
                -- handle error if we are not lucky
            end
        end -- if

        ctx._orc_channels[dat] = dat
    end -- for

    ngx.ctx = ctx
end

local function _default(val, err_return)
    if val == nil then
        return err_return
    end -- if
end _default

function _M.client_on_abort_cb()
    local ctx = ngx.ctx
    local channel_dict = ngx.shared[self.final['channel_dict']]

    if type(ctx._orc_channels) == "table" and table.getn(ctx._orc_channels) then
        for dex, dat in ctx._orc_channels do
            local ok, err = channel_dict:incr(dat, -1)
            if ok > 0 then -- we are to low now
                channel_dict:incr(dat, 1)
            end -- if
        end --for
    end -- if
end

-- handles requests that come though the upstream request because
-- we had a push come in
function _M.nchan_publish_upstream_proxy()
    local req_headers = ngx.req.get_headers()
    local channel = req_headers['X-Event-Source-Event']
    local c_name = nil
    local c_host = nil
    local r_chan = nil

    if  channel == nil then
        channel = "*@*"
    end --if

    r_chan = string.reverse(channel) --reverse the hostname since we need last @
    local pos = r_chan:strfind("@")
    if pos == nil then
        c_name = channel
    else
        c_host = string.reverse(r_chan:sub(0, pos))
        c_name = string.reverse(r_chan:sub(pos, string.len(r_chan)))
    end

    -- read the channel that this message is for

    -- check if this message is suppose to be localhost
    if c_host == "localhost" or c_host == self.config:get('system')['fqdn'] then
        -- route this message internally only
        ngx.exit(ngx.HTTP_NOT_MODIFIED ) -- this will route the orginal message internally
        return

    -- check if this is reserved channel name
    elseif c_host == nil then
        -- lookup where this should be

    -- check if the message is broadcast to all nodes in cluster
    elseif c_host == "*" then
        local hosts = self:cluster_get_hosts()
        hosts[self.config:get('system')['fqdn']] = nil
        for dex, dat in hosts do
            ngx.timer.at(0, self.nchan_do_upstream_proxy, dat['cluster_host'], dat['cluster_port']) -- setup workers to push to all nodes
        end -- for

        ngx.exit(ngx.HTTP_NOT_MODIFIED )
        return

    elseif c_host:find("*") == nil then -- check if host is fixed string match
        -- send to just a single host on the list

        return
    elseif else c_host:find("*") ~= nil then
        -- try to match what hosts this should be
        local hosts = self:cluster_get_hosts()
        local route_self= false -- set to true if we want to route message to our self
        local host_self = hosts[self.config:get('system')['fqdn']]

        for dex, dat in hosts do -- loop over all the hosts
            local host_alias_str = dex.." " -- set the main host as first
            for dex2, dat2 in hosts ['host_alias'] do
                host_alias_str = host_alias_str .." "..dex2 -- append
            end -- for

            -- todo: at some point want to load all of them into one string an then us ngx.re.gmatch to get a better list
            local cap, err = ngx.re.match(host_alias_str, c_host)
            if err then
                ngx.err(ngx.ERR, "Faild to route message because of key: "..host_alias_str .. "  " .. chost.."  "..err)
            end

            if cap ~= nil and dex ~= host_self then -- something in the list matched there for we can run with it
                ngx.timer.at(0, self.nchan_do_upstream_proxy, dat['cluster_host'], dat['cluster_port'])
            else cap ~= nil and dex == host_self then -- route the message to our self at the end of the block
                route_self = true
            end -- if
        end -- for

        -- we should route the message to our selves at the end of all of this
        if route_self == true then
            ngx.exit(ngx.HTTP_NOT_MODIFIED)
        end -- if
        return
    end -- if

    ngx.exit(ngx.HTTP_NO_CONTENT)
end -- function


function _M.nchan_do_upstream_proxy(self, premature, host, port)
    if premature then
        return
    end -- if

    local httpc = http:new()
    local res, err = httpc:request_uri("http://"..host..":"..port.."/nchan_proxy", {})
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


-- load my own system meta data
function system_load_meta()
    local meta = {}
    meta['nginx']["version"] = ngx.config.nginx_version
    meta['nginx']['worker_count'] = ngx.worker_count()
    meta['lua']['version'] = ngx.config.ngx_lua_version
    meta['linux']['hostname'] = string.gsub(io.popen ("/bin/hostname"):read("*a") or "", "\n$", "")
    meta['orc']['version'] = self.version

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

function _M.cluster_do_healthcheck(premature, self, host, port)
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
