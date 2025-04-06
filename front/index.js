$(function() {

    _requests_in_progress = 0;

    $.ajaxSetup({
        beforeSend: function (xhr) {
            _requests_in_progress += 1;
            $("#cb-server-request-progress-bar").show();
        }
    });

    CanvasJS.addColorSet("greenShades",
                //colorSet Array
["#14342B", "#60935D", "#BAB700", "#BBDFC5", "#FF579F", "#FA7921", "#FE9920", "#B9A44C",
"#566E3D", "#0C4767"
]

                );


    var swap_in_array = function(array, index1, index2) {
        var tmp = array[index1];
        array[index1] = array[index2];
        array[index2] = tmp;
    }
    
    var format_date = function(dateObject) {
        var d = new Date(dateObject);
        var day = d.getDate();
        var month = d.getMonth() + 1;
        var year = d.getFullYear();
        if (day < 10) {
            day = "0" + day;
        }
        if (month < 10) {
            month = "0" + month;
        }
        var date = year + "-" + month + "-" + day;

        return date;
    };

    function isObject(obj) {
        var type = typeof obj;
        return type === 'function' || (type === 'object' && !!obj);
    }

    $(document).on("ajaxComplete", function() {
        if (_requests_in_progress > 0) {
            _requests_in_progress -= 1;
        }
        if (!_requests_in_progress) {
            $("#cb-server-request-progress-bar").hide();
        }
    });

    function isDateValid(dateStr) {
        return !isNaN(new Date(dateStr));
    }

    $("body").on('click', ".tui-tab", function() {
        var $this = $(this);
        CB.make_tab_active($this);
    });

    $("#cb-planning-form").on('change', "input,select", function() {
        CB._FORM_CLEAN = false;
    });

    $("#cb-jira-form").on('change', "input,select", function() {
        CB._FORM_CLEAN = false;
    });

    $("#cb-metric-form").on('change', "input,select", function() {
        CB._FORM_CLEAN = false;
    });

    $("#cb-dashboard-form").on('change', ".cb-name-input", function() {
        CB._FORM_CLEAN = false;
    });

    $("body").on('change', "#cb-metric-form .cb-stage-tabs .cb-variables input", function(event) {
        var active_block = $(event.target).parents('.cb-variables');

        var i;
        for (i=0; i<event.target.classList.length; ++i) {
            if (event.target.classList[i] == 'cb-datetime') {
                return;
            }
        }
        CB.sync_metric_variables(active_block);
    });

    function exclude_from_array(arr, item) {
        for (var i = arr.length; i--;) {
            if (arr[i] === item) arr.splice(i, 1);
       }
    }

    var URLS = {
        dashboards: {
            list: GLOBALS.BACKEND_HOST + '/metric/dashboards',
            delete: GLOBALS.BACKEND_HOST + '/metric/dashboards',
            save: GLOBALS.BACKEND_HOST + '/metric/dashboards'
        },
        metrics: {
            options: GLOBALS.BACKEND_HOST + '/metric/options',
            list: GLOBALS.BACKEND_HOST + '/metric/metrics',
            save: GLOBALS.BACKEND_HOST + '/metric/metrics',
            delete: GLOBALS.BACKEND_HOST + '/metric/metrics',
            dry_run: GLOBALS.BACKEND_HOST + '/metric/pipeline/dry-run',
            run: GLOBALS.BACKEND_HOST + '/metric/pipeline/run'
        },
        jiras: {
            delete: GLOBALS.BACKEND_HOST + '/sources/jiras',
            list: GLOBALS.BACKEND_HOST + '/sources/jiras',
            save: GLOBALS.BACKEND_HOST + '/sources/jiras',
            indexify: GLOBALS.WS_BACKEND_HOST + '/sources/jiras/indexify'
        },
        redmines: {
            delete: GLOBALS.BACKEND_HOST + '/sources/redmines',
            list: GLOBALS.BACKEND_HOST + '/sources/redmines',
            save: GLOBALS.BACKEND_HOST + '/sources/redmines',
            indexify: GLOBALS.WS_BACKEND_HOST + '/sources/redmines/indexify'
        },
        issues: {
            options: GLOBALS.BACKEND_HOST + '/sources/issues/options'
        },
        planning: {
            list: GLOBALS.BACKEND_HOST + "/planning/plannings",
            calendar: GLOBALS.BACKEND_HOST + "/planning/calendar",
            save: GLOBALS.BACKEND_HOST + "/planning/plannings",
            delete: GLOBALS.BACKEND_HOST + "/planning/plannings",
            save_done_percents: GLOBALS.BACKEND_HOST + "/planning/done_percents",
            filter_done_percents: GLOBALS.BACKEND_HOST + "/planning/done_percents",
            get_done_percent: GLOBALS.BACKEND_HOST + "/planning/done_percent",
            history: GLOBALS.BACKEND_HOST + "/planning/history",
            get_dayoffs: GLOBALS.BACKEND_HOST + "/planning/dayoffs"
        }
    };

    CB = {}

    CB.regexExecAll = (str, regex) => {
        var lastMatch;
        var matches = [];

        while ((lastMatch = regex.exec(str))) {
            matches.push(lastMatch);

            if (!regex.global) break;
        }

        return matches;
    }; 


    CB.process_http_error = function(jq, status, error) {
        var text = JSON.stringify(jq.responseJSON);
        console.log(`Server error: json=${text}, error=${error}`);
        if (error == "Forbidden") {
            CB.show_popup("Forbidden", jq.responseJSON.detail);
        } else {
            CB.show_popup("Request failed", text);
        }
    }

    CB.init_timezone_selects = function () {
        var $select = $('.cb-timezone');

    }

    CB.init_timezone_selects();

    function getTimezoneOffset(timeZone) {
        const now = new Date();
        const tzString = now.toLocaleString('en-US', { timeZone });
        const localString = now.toLocaleString('en-US');
        const diff = (Date.parse(localString) - Date.parse(tzString)) / 3600000;
        const offset = diff + now.getTimezoneOffset() / 60;
        return -offset;
    }

    CB.render_source_status = function(status) {
        status = status.toUpperCase();
        if (status == 'INDEXING') {
            return "<img class='cb-status-icon' src='/front/images/doom-unknown.png'><br><span class='cb-index-status'>IN PROGRESS<span>"
            return '&#129300;';
        } else if (status == 'ERROR') {
            return "<img class='cb-status-icon' src='/front/images/doom-bad.png'><br><span class='cb-index-status'>ERROR</span>"
            return '&#128565;';
        } else if (status == 'INDEXED') {
            return "<img class='cb-status-icon' src='/front/images/doom-smile.png'><br><span class='cb-index-status'>INDEXED</span>"
            return '&#128512;';
        }

    }

    CB.render_redmines_table = function() {
        var clean = CB.confirm_form_close(function() {
            CB.render_redmines_table();
        });

        if (!clean) {
            return;
        }

        location.hash = "redmines";
        CB.set_active_menu("#cb-sources-menu");

        CB.hide_all_innerblocks();
        var $panel = $("#cb-redmines-table");
        var $tbody = $panel.find('tbody')

        $.ajax({
            url: URLS.redmines.list,
            dataType: 'json',
            crossDomain: true,

            success: function(redmines) {
                $tbody.empty();

                var i;
                for(i=0; i<redmines.length; i++) {
                    var d = redmines[i];

                    var status_html = CB.render_source_status(d.status);
                    $tbody.append(
                        "<tr onclick=\"CB.render_redmine_form('" + d.internal_id + "');\">" +
                        `<td class='${CB.get_source_status_class(d.status)}'>` + status_html + "</td>" +
                        "<td>" + d.name + "</td>" +
                        "<td>" + d.url + "</td>" +
                        "</tr>"
                    );
                }
                $panel.show(100);
            },
            error: CB.process_http_error
        });
    }

    CB.render_jiras_table = function() {
        var clean = CB.confirm_form_close(function() {
            CB.render_jiras_table();
        });

        if (!clean) {
            return;
        }

        location.hash = "jiras";
        CB.set_active_menu("#cb-sources-menu");

        CB.hide_all_innerblocks();
        var $panel = $("#cb-jiras-table");
        var $tbody = $panel.find('tbody')

        $.ajax({
            url: URLS.jiras.list,
            dataType: 'json',
            crossDomain: true,

            success: function(jiras) {
                $tbody.empty();

                var i;
                for(i=0; i<jiras.length; i++) {
                    var d = jiras[i];

                    var status_html = CB.render_source_status(d.status);
                    $tbody.append(
                        "<tr onclick=\"CB.render_jira_form('" + d.internal_id + "');\">" +
                        `<td class='${CB.get_source_status_class(d.status)}'>` + status_html + "</td>" +
                        "<td>" + d.name + "</td>" +
                        "<td>" + d.url + "</td>" +
                        "</tr>"
                    );
                }
                $panel.show(100);
            },
            error: CB.process_http_error
        });
    }

    CB.onchange_redmine_auth_method = function() {
        var $form = $("#cb-redmine-form");
        var method = $form.find(".cb-auth-method-input").val();

        if (method == 'basic') {
            $form.find(".cb-auth-method-basic-section").show();
            $form.find(".cb-auth-method-token-section").hide();
        } else {
            $form.find(".cb-auth-method-basic-section").hide();
            $form.find(".cb-auth-method-token-section").show();
        }
    }

    CB.render_redmine_form = function(internal_id, preserve_before_update) {
        var clean = CB.confirm_form_close(function() {
            CB.render_redmine_form(internal_id, preserve_before_update);
        });

        if (!clean) {
            return;
        }

        location.hash = `redmine:${internal_id || ''}`;
        CB.set_active_menu("#cb-sources-menu");

        var $form = $("#cb-redmine-form");
        var $legend = $("#cb-redmine-form > legend");

        if (!preserve_before_update) {
            CB.empty_redmine_form();
        }

        var _finish_form = function() {
            CB.hide_all_innerblocks();
            var $panel = $("#cb-redmine-form");

            CB.onchange_redmine_auth_method();
            $panel.show(100);
        }

        if (internal_id) {
            $.ajax({
                url: URLS.redmines.list + "/" + internal_id,

                dataType: 'json',
                crossDomain: true,

                success: function(redmine) {
                    $legend.text("Redmine [" + redmine.name + "]");
                    $form.find(".cb-internal-id-input").val(redmine.internal_id);
                    $form.find(".cb-name-input").val(redmine.name);
                    $form.find(".cb-url-input").val(redmine.url);
                    $form.find(".cb-login-input").val(redmine.login);
                    $form.find(".cb-password-input").val(redmine.password);
                    $form.find(".cb-auth-method-input").val(redmine.auth_method || 'basic');
                    $form.find(".cb-token-input").val(redmine.token);
                    $form.find(".cb-projects-input").val(
                        (redmine.projects || []).join("\n")
                    );
                    $form.find(".cb-index-period-input").val(redmine.index_period);
                    $form.find(".cb-index-status-input").val(redmine.status);

                    $form.find(".cb-indexed-at-input").val(
                        iso_to_human(redmine.indexed_at, 'UTC')
                    );

                    var i;
                    for (i=0; i<redmine.logs.length; ++i) {
                        CB.add_redmine_log(redmine.logs[i]);
                    }

                    if (!redmine.logs.length) {
                        var $log_container = $("#cb-redmine-indexify-log");
                        $log_container.html("No logs yet...");
                    }
                    _finish_form();
                },
                error: CB.process_http_error
            });

        } else {
            $legend.html("Creating a new redmine source");
            _finish_form();
        }
    }

    CB.close_popup = function(el) {
        if (!el) {
            $("#cb-popup").hide();
        } else {
            $(el).parents('.cb-popup').hide();    
        }
    }

    CB.close_server_progress_bar = function() {
        _requests_in_progress = 0;
        $("#cb-server-request-progress-bar").hide();
    }

    CB.show_popup = function(header, text) {
        var popup = $("#cb-popup");
        popup.find(".tui-panel-header").html(header);
        popup.find(".cb-popup-text").html(text);
        popup.show();
    }

    CB.make_tab_active = function(li) {
        $this = $(li);
        $this.closest('.tui-panel').find("> .content").find("> .tui-tab-content").hide();
        $this.closest('.tui-panel').find("> .tui-tabs .tui-tab").removeClass('active');

        $this.addClass('active');
        var content_id = $this.attr('data-tab-content');
        $("#" + content_id).show();

        var editors = $("#" + content_id).find('.cb-action-value');
        if(editors.length) {
            var editor = editors[0];
            if (editor.value) {
                editor.value = editor.value;
            }
        }
    }


    function get_random_id() {
        var letters = '0123456789ABCDEF';
        var id = '';
        for (var i = 0; i < 3; i++) {
            id += letters[Math.floor(Math.random() * letters.length)];
        }
        return id;
    }

    function _update_current_time() {
        var now = new Date();
        var year = now.getFullYear();
        var month = now.getMonth() + 1;
        var day = now.getDate();

        var hours = now.getHours();
        var minutes = now.getMinutes();
        var seconds = now.getSeconds();

        $("#cb-current-date").html(
            year + "/" +
            String(month).padStart(2, '0') + "/" +
            String(day).padStart(2, '0')
        );
        $("#cb-current-time").html(
            String(hours).padStart(2, '0') + ":" +
            String(minutes).padStart(2, '0') + ":" +
            String(seconds).padStart(2, '0')
        );

        setTimeout(_update_current_time, 1000);
    }
    setTimeout(_update_current_time, 1000);

    function iso_to_human(iso, tz) {
        if (!iso) {
            return '';
        }
        var dt = remove_iso_t(cut_iso_till_minutes(iso));
        if (tz) {
            dt += ' ' + tz;
        }
        return dt;
    }

    function to_iso_date(datetime) {
        var s = datetime.toISOString();
        var chunks = s.split("T");
        if (chunks.length > 1) {
            return chunks[0];
        } else {
            return iso;
        }
    }

    function to_iso_only_minutes(datetime) {
        var s = datetime.toISOString();
        return cut_iso_till_minutes(s);
    }

    function remove_iso_t(iso) {
        var chunks = iso.split("T")
        if (chunks.length > 1) {
            return chunks[0] + " " + chunks[1];
        } else {
            return iso;
        }
    }

    function cut_iso_till_minutes(iso) {
        var chunks = iso.split(":")
        return chunks[0] + ":" + chunks[1];
    }

    function initiate_datetime_inputs() {
        var now = new Date();
        $(".cb-datetime-now").val(to_iso_only_minutes(now));

        now.setDate(now.getDate() - 30);
        $(".cb-datetime-week-ago").val(to_iso_only_minutes(now));
    }

    function contains(array, item, eq) {
        eq = eq || function(item1, item2) {
            return item1 == item2;
        }

        var i;
        for(i=0; i<array.length; ++i) {
            if (eq(array[i], item)) {
                return true;
            }
        }
        return false;
    }
    initiate_datetime_inputs();

    CB.add_new_stage_input_onclick = function(button) {
        CB.add_stage_input(
            {},
            $(button).parents('.cb-stage-inputs')
        );
    }

    CB.refresh_redmine_logs_onclick = function() {
        var $form = $("#cb-redmine-form");
        var internal_id = $form.find(".cb-internal-id-input").val();
        if (!internal_id) {
            return;
        }

        $.ajax({
            url: URLS.redmines.list + "/" + internal_id,

            dataType: 'json',
            crossDomain: true,

            success: function(redmine) {
                $("#cb-redmine-indexify-log").empty();

                var i;
                for (i=0; i<redmine.logs.length; ++i) {
                    CB.add_redmine_log(redmine.logs[i]);
                }

                if (!redmine.logs.length) {
                    var $log_container = $("#cb-redmine-indexify-log");
                    $log_container.html("No logs yet...");
                }
            },
            error: CB.process_http_error
        });
    }

    CB.refresh_jira_logs_onclick = function() {
        var $form = $("#cb-jira-form");
        var internal_id = $form.find(".cb-internal-id-input").val();
        if (!internal_id) {
            return;
        }

        $.ajax({
            url: URLS.jiras.list + "/" + internal_id,

            dataType: 'json',
            crossDomain: true,

            success: function(jira) {
                $("#cb-jira-indexify-log").empty();

                var i;
                for (i=0; i<jira.logs.length; ++i) {
                    CB.add_jira_log(jira.logs[i]);
                }

                if (!jira.logs.length) {
                    var $log_container = $("#cb-jira-indexify-log");
                    $log_container.html("No logs yet...");
                }
            },
            error: CB.process_http_error
        });
    }

    CB.indexify_redmine_onclick = function(button, full) {
        var $button = $(button);

        var internal_id = $("#cb-redmine-form").find(".cb-internal-id-input").val();
        if (!internal_id || !CB._FORM_CLEAN) {
            CB.show_popup('Cannot do it', 'Changes are not saved yet. Click Save button before proceeding. ');
            return;
        }

        $("#cb-redmine-indexify-progress-bar").css("visibility", "visible");

        var ws_proto = 'wss';
        if (location.protocol == 'http:') {
            ws_proto = 'ws';
        }

        var ws_url = `${ws_proto}://${location.host}${URLS.redmines.indexify}`
        let socket = new WebSocket(ws_url);

        socket.onopen = function(e) {
            socket.send(internal_id + "_" + (full? 'full': 'delta'));
        };

        socket.onmessage = function(event) {
            console.log(`[message] Данные получены с сервера: ${event.data}`);
            var log = JSON.parse(event.data);
            CB.add_redmine_log(log, true);
        };

        socket.onclose = function(event) {
            $("#cb-redmine-indexify-progress-bar").css("visibility", "hidden");
            if (event.wasClean) {
                console.log(`[close] Соединение закрыто чисто, код=${event.code} причина=${event.reason}`);
            } else {
                // например, сервер убил процесс или сеть недоступна
                // обычно в этом случае event.code 1006
                var error = 'Соединение прервано. Ошибка на сервере.';
                CB.show_popup("Request failed", error);
            }
            CB.render_redmine_form(internal_id, true);
        };

        socket.onerror = function(error) {
            $("#cb-redmine-indexify-progress-bar").css("visibility", "hidden");
            CB.show_popup("Request failed", error);
        };
    }

    CB.indexify_jira_onclick = function(button, full) {
        var $button = $(button);

        var internal_id = $("#cb-jira-form").find(".cb-internal-id-input").val();
        if (!internal_id || !CB._FORM_CLEAN) {
            CB.show_popup('Cannot do it', 'Changes are not saved yet. Click Save button before proceeding. ');
            return;
        }

        $("#cb-jira-indexify-progress-bar").css("visibility", "visible");

        var ws_proto = 'wss';
        if (location.protocol == 'http:') {
            ws_proto = 'ws';
        }

        var ws_url = `${ws_proto}://${location.host}${URLS.jiras.indexify}`
        let socket = new WebSocket(ws_url);

        socket.onopen = function(e) {
            socket.send(internal_id + "_" + (full? 'full': 'delta'));
        };

        socket.onmessage = function(event) {
            console.log(`[message] Данные получены с сервера: ${event.data}`);
            var log = JSON.parse(event.data);
            CB.add_jira_log(log, true);
        };

        socket.onclose = function(event) {
            $("#cb-jira-indexify-progress-bar").css("visibility", "hidden");
            if (event.wasClean) {
                console.log(`[close] Соединение закрыто чисто, код=${event.code} причина=${event.reason}`);
            } else {
                // например, сервер убил процесс или сеть недоступна
                // обычно в этом случае event.code 1006
                var error = 'Соединение прервано. Ошибка на сервере.';
                CB.show_popup("Request failed", error);
            }
            CB.render_jira_form(internal_id, true);
        };

        socket.onerror = function(error) {
            $("#cb-jira-indexify-progress-bar").css("visibility", "hidden");
            CB.show_popup("Request failed", error);
        };
    }

    CB.update_input_type_details_inputs = function(select, stage_input) {
        var $type = $(select);
        var $group = $(select).parents(".cb-input-group");
        var $details = $group.find('.cb-input-type-details');

        $details.empty();

        if ($type.val() == 'db') {
            $details.append(
                "<span>Collection name.....: </span>"
            );
            $collection_name = $(
            "<select class='tui-input cb-input-collection-name-input' >" +
            "<option value='datasource_jira_issues_public'>Jira issues</option>" +
            "<option value='datasource_jira_sprints_public'>Jira sprints</option>" +
            "<option value='datasource_redmine_issues_public'>Redmine issues</option>" +
            "</select>"
            );
            $collection_name.val(stage_input.collection_name || '');
            $details.append($collection_name);

            $docs = $(
                "<a class='cb-input-collection-doc' target='_blank' href='https://google.com'>?</a>"
            );
            $docs.attr("href", "https://github.com/panaetov/captain-bridge/wiki/Indexed-Data")
            $details.append($docs);
            $details.append('<br>');
        }

        if ($type.val() == 'stage') {
            $details.append(
                "<span>Stage name..........: </span>"
            );
            $stage_name = $(
                "<select class='cb-input-stage-name-input tui-input' value=''></select>"
            );

            if (stage_input.stage_name) {
                $stage_name.append("<option>" + stage_input.stage_name + "</option>")
                $stage_name.val(stage_input.stage_name || '');
            }
            $details.append($stage_name);
            $details.append("<br>");
        }

        $details.append(
            "<span>Alias...............: </span>"
        );
        $alias_name = $(
            "<input class='cb-input-alias-input tui-input' value='' />"
        );
        $alias_name.val(stage_input.alias || '');
        $details.append($alias_name);
    }

    CB.onchange_input_type = function(button) {
        CB.update_input_type_details_inputs(button, {});
        CB.actualize_stage_inputs();
    }

    CB.add_stage_input = function(stage_input, $inputs_elem) {
        var $group = $("<div class='cb-input-group'></div>");
        $inputs_elem.find('.cb-add-new-input').before($group);

        $group.append(
            "Input type..........: "
        );
        var $type = $(
            "<select class='tui-input cb-input-type-input' " +
            "onchange='CB.onchange_input_type(this);'>" +
            "<option>stage</option>" +
            "<option>db</option>" +
            "</select>"
        );
        $type.val(stage_input.input_type || 'db');
        $group.append($type);
        $group.append($(
            "<br><div class='cb-input-type-details'></div>"
        ));
        CB.update_input_type_details_inputs($type, stage_input);

        $group.append(
            "<button class=\"tui-button red-168 cb-delete-input\" onclick=\"CB.delete_input_on_click(this);\">X</button>"
        );
    }

    CB.update_action_details_inputs = function(action_type, stage) {
        stage = stage || {action: {}};
        var $type = $(action_type);
        var $stage = $type.parents(".cb-stage");
        var $details = $stage.find('.cb-action-details');

        $details.empty();

        if ($type.val() == 'query') {
            $details.append(
                "<span class='cb-float-left'>Pipeline............:&nbsp</span>"
            );

            var source = JSON.stringify(
                stage.action.pipeline || [{
                    "$match": {
                        "created": {
                            "$gt": "$$datetime_from",
                            "$lte": "$$datetime_to"
                        }
                    }
                }], null, 4
            );
            $pipeline = $(
                "<wc-codemirror class='cb-action-value cb-action-pipeline' mode='javascript' >" +
                "<script type='wc-content'>" +
                source +
                "</script>" +
                "</wc-codemirror>"
            );
            pipeline = $pipeline[0];

            try {
                $pipeline[0].connectedCallback();
            } catch (error) {
            }
            pipeline.value = source;
            $details.append($pipeline);
        }

        if ($type.val() == 'rpc') {
            $details.append(
                "<span>URL.................: </span>"
            );
            $url = $(
                "<input class='cb-action-url cb-action-value tui-input' />"
            );
            $url.val(stage.action.url || '');
            $details.append($url);
        }

        if ($type.val() == 'python') {
            $details.append(
                "<span class='cb-float-left'>Code................:&nbsp</span>"
            );
            var source = stage.action.code || "print(INPUTS)\n\nreturn []";
            $pipeline = $(
                "<wc-codemirror class='cb-action-value cb-action-code' mode='python' >" +
                "<script type='wc-content'>" +
                "\n" +
                source +
                "\n" +
                "</script>" +
                "</wc-codemirror>"
            );
            $pipeline[0].connectedCallback();
            $details.append($pipeline);
        }
    }

    CB.delete_input_on_click = function(button) {
        $(button).parents('.cb-input-group').remove();
    }

    CB._FORM_CLEAN = true;
    CB._CONFIRM_YES_CALLBACK = null;

    CB.confirm_form_close_on_yes = function() {
        CB.close_popup("#cb-confirm-saving-popup .cb-yes-button");
        CB._FORM_CLEAN = true;
        if (CB._CONFIRM_YES_CALLBACK) {
            CB._CONFIRM_YES_CALLBACK();
        }
    } 
    
    CB.confirm_form_close = function(yes_cb) {
        if (!CB._FORM_CLEAN) {
            CB._CONFIRM_YES_CALLBACK = yes_cb;
            var popup = $("#cb-confirm-saving-popup");
            popup.show();
            return;
        }
        return CB._FORM_CLEAN;
    }

    CB.hide_all_innerblocks = function() {
        $("#cb-mainblock").show();
        $(".cb-innerblock").hide();
    }

    CB.parse_metric_stage = function($stage_form) {
        var name = $stage_form.find('.cb-stage-name-input').val();

        var terminal_name = $stage_form.parents(".cb-stages-list").find('.cb-terminal-stage-input').val();
        var is_terminal = (name == terminal_name);

        var $input_forms = $stage_form.find('.cb-input-group');

        var inputs = [];
        var i;
        for (i=0; i<$input_forms.length; ++i) {
            var $input_form = $($input_forms[i]);

            var input_type = $input_form.find(".cb-input-type-input").val()
            var collection_name = $input_form.find(".cb-input-collection-name-input").val();  
            var substage_name = $input_form.find('.cb-input-stage-name-input').val();
            var alias = $input_form.find('.cb-input-alias-input').val();

            inputs.push({
                input_type: input_type,
                collection_name: collection_name,
                stage_name: substage_name,
                alias: alias
            })
        }

        function get_code(klass, default_value) {
            var inputs = $stage_form.find(klass);
            if (inputs.length) {
                return inputs[0].value;
            } else {
                return default_value;
            }
        }

        var action = {
            pipeline: JSON.parse(get_code('.cb-action-pipeline', '[]')),
            action_type: $stage_form.find('.cb-action-type-input').val(),
            url: $stage_form.find('.cb-action-url').val(),
            code: get_code('.cb-action-code', '')
        }

        return {
            name: name,
            is_terminal: is_terminal,
            inputs: inputs,
            action: action
        }
    }
    
    CB.add_metric_error = function(error_text) {
        var $errors = $('#cb-metric-form-errors'); 

        var error = "<span class='cb-bold'>ERROR: </span><span>" + error_text + "</span><br/>"; 
        $errors.append(error);

        var $table = $('.cb-stage-results');
        $table.empty();
        $table.html(error);
    }

    CB.validate_metric_form = function() {
        var $form = $("#cb-metric-form");
        var $errors = $('#cb-metric-form-errors'); 
        $errors.empty();

        var internal_id = $form.find(".cb-internal-id-input").val();

        var name = $form.find(".cb-name-input").val();
        if (!name) {
            CB.add_metric_error("Name is empty.");
            return false;
        }

        var terminal_stage = $form.find(".cb-terminal-stage-input").val();
        if (!terminal_stage) {
            CB.add_metric_error("Name of terminal stage is empty.");
            return false;
        }

        var stages = $form.find(".cb-stage-tabs");
        var stage_forms = stages.find('.cb-stage');

        var i;
        for(i=0; i<stage_forms.length; ++i) {
            var $stage_form = $(stage_forms[i]);
            var tab_id = $stage_form.attr('id');
            var tab_header = stages.find("[data-tab-content=" + tab_id + "]");
            var tab_title = tab_header.html();

            if (!$stage_form.find('.cb-stage-name-input').val()) {
                CB.add_metric_error("Name of stage '" + tab_title + "' is empty.");
                return false;
            }

            var j;

            var input_groups = $stage_form.find('.cb-input-group');
            if (!input_groups.length) {
                CB.add_metric_error("Stage '" + tab_title + "': no inputs.");
                return false;
            }

            for(j=0; j<input_groups.length; ++j) {
                $group = $(input_groups[j]);

                var input_type = $group.find('.cb-input-type-input').val();

                if (input_type == 'db') {
                    if (!$group.find('.cb-input-collection-name-input').val()) {
                        CB.add_metric_error("Stage '" + tab_title + "': empty collection name.");
                        return false;
                    }
                } else {
                    if (!$group.find('.cb-input-stage-name-input').val()) {
                        CB.add_metric_error("Stage '" + tab_title + "': empty stage name.");
                        return false;
                    }
                }
            }
        var $action_type = $stage_form.find('.cb-action-type-input');
        if ($action_type.val() == 'query') {
            var $pipeline = $stage_form.find('.cb-action-pipeline');

            if (!$pipeline.val()) {
                CB.add_metric_error("Stage '" + tab_title + "': action has empty pipeline.");
                return false;
            }

            try {
                JSON.parse($pipeline.val());
            } catch (e) {
                CB.add_metric_error("Stage '" + tab_title + "': " + e);
                return false;
            }

        }

        if ($action_type.val() == 'python') {
            var $code = $stage_form.find('.cb-action-code');

            if (!$code.val()) {
                CB.add_metric_error("Stage '" + tab_title + "': action has empty code.");
                return false;
            }
        }

        if ($action_type.val() == 'rpc') {
            var $url = $stage_form.find('.cb-action-url');

            if (!$url.val()) {
                CB.add_metric_error("Stage '" + tab_title + "': action has empty url.");
                return false;
            }

        }

        }

        return true;
    }

    CB.add_metric_variable_onclick = function() {
        CB._FORM_CLEAN = false;

        CB.add_metric_variable_spec();
        CB.configure_metric_debug_variables();
    }

    CB.add_metric_variable_spec = function(name, type) {
        var tbody = $("#cb-metric-variables-list .cb-variables-table tbody");

        var qty = $(tbody).find('tr').length; 

        name = name || `var_${qty - 1}`;
        type = type || 'text';

        var tr = $("<tr class='cb-metric-variable-row'></tr>");
        tr.append(`<td><input type='text' onchange='CB.configure_metric_debug_variables()' value='${name}'></td>`)
        tr.append(
            `<td><select value='${type}' onchange='CB.configure_metric_debug_variables()'>` +
            "<option>text</option>" +
            "<option>number</option>" +
            "<option>datetime</option>" +
            "</select></td>"
        );
        tr.append("<td class='cb-delete-button-container'><button class='' onclick='CB.delete_metric_variable_onclick(this);'>X</td>")
        tbody.append(tr);
    }

    CB.configure_metric_debug_variables = function() {
        var trs = $("#cb-metric-variables-list .cb-variables-table tbody tr");
        var blocks = $('.cb-variables');
        var j;
        for(j=0; j<blocks.length; ++j) {
            var block = blocks[j];

            var variables = $(block).find(".cb-variable");
            var k;
            for(k=2; k< variables.length; ++k) {
                $(variables[k]).remove();
            }
        }

        var i;
        for(i=2; i<trs.length; ++i) {
            var tr = trs[i];

            var tds = $(tr).find('td');
            var name = $(tds[0]).find('input').val();
            var type = $(tds[1]).find('select').val();

            var blocks = $('.cb-variables');
            var j;
            for(j=0; j<blocks.length; ++j) {
                var block = blocks[j];
                CB.add_metric_variable($(block), name, type);
            }
        }
    }

    CB.sync_metric_variables = function(active_block) {
        var $block = $(active_block);

        var all_blocks = $block.parents('.cb-stage-tabs-content').find('.cb-variables');
        var i;
        for(i=0; i<all_blocks.length; ++i) {
            $other_block = $(all_blocks[i]);
            $other_block.replaceWith($block.clone());

        }
    }

    CB.add_metric_variable = function(containers, name, type) {
        var container = $("<div class='cb-variable'></div>");
        containers.append(container);

        var name = $(`<input disabled class='tui-input cb-variable-name' value=${name}>`);
        container.append(name);

        container.append(' = ');

        if (type == 'datetime') {
            var select = $(
                "<input class='cb-datetime cb-datetime-from cb-datetime-now' " +
                "type='datetime-local'></input>"
            );
            container.append(select);

        } else if (type == 'number'){

            var select = $(
                "<input class='cb-variable-value' " +
                "type='number'></input>"
            );
            container.append(select);

        } else {
            var select = $("<input class='cb-variable-value'></input>");
            container.append(select);
        }
        container.append("<br/>");
    }

    CB.add_dashboard_variable_onclick = function() {
        CB.add_dashboard_variable();
    }

    CB.delete_metric_variable_onclick = function(button) {
        CB._FORM_CLEAN = false;

        var tr = $(button).parents('.cb-metric-variable-row');
        tr.remove();

        CB.configure_metric_debug_variables();
    }

    CB.add_dashboard_variable = function(data) {
        data = data || {};
        var containers = $("#cb-variables");
        var current_vars = containers.find('.cb-variable');
        var i;
        var current_variable_block = null;
        for(i=0; i<current_vars.length; ++i) {
            var current_name = $(current_vars[i]).find('.cb-variable-name').val();
            if (current_name == data.name) {
                current_variable_block = $(current_vars[i]);
            }
        }

        var container;
        if (current_variable_block) { 
            if (data.type == 'datetime') {
                container = current_variable_block;
                container.empty();
            } else {
                return;
            }

        } else {
            container = $("<div class='cb-variable'></div>");
            containers.append(container);
        }

        var name = $("<input disabled class='tui-input cb-variable-name'>");
        container.append(name);
        name.val(data.name || '');

        container.append(' = ');

        if (data.type == 'datetime') {
            var select = $(
                "<input class='cb-datetime cb-variable-value' " +
                "type='datetime-local'></input>"
            );
            container.append(select);

        } else {
            var select = $("<select class='cb-variable-value'></select>");
            container.append(select);

            var textarea = $("<textarea class='tui-input cb-variable-options'></textarea>");
            container.append(textarea);

            container.append("<button onclick='CB.edit_dashboard_variable_option_onclick(this);'>&#9477;</button>");
            container.append("<br/>");

            var i;
            for(i=0; i<(CB.DASHBOARD.variables || []).length; ++i) {
                var vv = CB.DASHBOARD.variables[i];
                if (vv.name == data.name) {
                    data.options = vv.options;
                }
            }

            if (data.options) {
                textarea.val(data.options);
                var options = data.options.split("\n");
                for(i=0; i<options.length; ++i) {
                    select.append(`<option>${options[i]}</options>`);
                }
            }
        }
    }

    CB.edit_dashboard_variable_option_onclick = function(button) {
        var container = $(button).parents(".cb-variable")[0];

        var containers = $("#cb-variables .cb-variable");
        var i;
        var current_options = '';
        var line_num = -1;
        for(i=0; i<containers.length; ++i) {
            var c = containers[i];
            if (c == container) {
                line_num = i;
                current_options = $(container).find('.cb-variable-options').val();
            }
        }

        var popup = $("#cb-dashboard-variable-options");

        popup.find('.cb-value').val(current_options);
        popup.find('.cb-line-num').val(line_num);

        popup.show();
    }

    CB.save_variable_options = function() {
        CB._FORM_CLEAN = false;

        var popup = $("#cb-dashboard-variable-options");

        var options_val = popup.find('.cb-value').val();
        var line_num = parseInt(popup.find('.cb-line-num').val());

        var container = $("#cb-variables .cb-variable")[line_num];

        container = $(container);

        container.find('.cb-variable-options').val(options_val);
        var select = $(container.find('.cb-variable-value'));
        select.empty();

        var options = options_val.split("\n");
        for(i=0; i<options.length; ++i) {
            select.append(`<option>${options[i]}</options>`);
        }
        popup.hide();
    }

    CB.delete_dashboard_variable_option_onlick = function(button) {
        $(button).parents('.cb-variable').remove();
    }

    CB.delete_dashboard_onclick = function() {
        var popup = $("#cb-delete-dashboard-popup");
        popup.show();
    }

    CB.delete_dashboard = function() {
        var popup = $("#cb-delete-dashboard-popup");
        popup.hide();

        $form = $("#cb-dashboard-form");
        var internal_id = $form.find(".cb-internal-id-input").val();

        if (internal_id) {
            $.ajax({
                url: URLS.dashboards.delete + "/" + internal_id,
                method: 'delete',
                crossDomain: true,

                success: function(result) {
                    CB.show_popup('OK', 'Dashboard is deleted');
                    CB.render_dashboards_table();
                },
                error: CB.process_http_error
            });
        } else {
            CB.show_popup('OK', 'Dashboard is deleted');
            CB.render_dashboards_table();
        }
    }

    CB.delete_jira_onclick = function() {
        var popup = $("#cb-delete-jira-popup");
        popup.show();
    }

    CB.delete_jira = function() {
        var popup = $("#cb-delete-jira-popup");
        popup.hide();

        $form = $("#cb-jira-form");
        var internal_id = $form.find(".cb-internal-id-input").val();

        if (internal_id) {
            $.ajax({
                url: URLS.jiras.delete + "/" + internal_id,
                method: 'delete',
                crossDomain: true,

                success: function(result) {
                    CB.show_popup('OK', 'Source is deleted');
                    CB.render_jiras_table();
                },
                error: CB.process_http_error
            });
        } else {
            CB.show_popup('OK', 'Source is deleted');
            CB.render_jiras_table();
        }
    }

    CB.delete_metric_onclick = function() {
        var popup = $("#cb-delete-metric-popup");
        popup.show();
    }

    CB.delete_metric = function() {
        var popup = $("#cb-delete-metric-popup");
        popup.hide();

        $form = $("#cb-metric-form");
        var internal_id = $form.find(".cb-internal-id-input").val();

        if (internal_id) {
            $.ajax({
                url: URLS.metrics.delete + "/" + internal_id,
                method: 'delete',
                crossDomain: true,

                success: function(result) {
                    CB.show_popup('OK', 'Metric is deleted');
                    CB.render_metrics_table();
                },
                error: CB.process_http_error
            });
        } else {
            CB.show_popup('OK', 'Metric is deleted');
            CB.render_metrics_table();
        }
    }

    CB.clone_metric = function() {
        $form = $("#cb-metric-form");

        var name = $form.find(".cb-name-input").val();
        var $stage_forms = $form.find('.cb-stage');

        var stages = [];
        for(i=0; i<$stage_forms.length; ++i) {
            var $stage_form = $($stage_forms[i]);
            stages.push(CB.parse_metric_stage($stage_form));
        }

        CB.render_metric_form('', {
            name: name,
            pipeline: stages
        });
    }

    CB.save_metric = function() {
        var is_valid = CB.validate_metric_form();
        if (!is_valid) {
            $("#cb-metric-form-errors").show();
            return;
        } else {
            $("#cb-metric-form-errors").hide();
        }

        $form = $("#cb-metric-form");

        var $stage_forms = $form.find('.cb-stage');

        var stages = [];
        for(i=0; i<$stage_forms.length; ++i) {
            var $stage_form = $($stage_forms[i]);
            stages.push(CB.parse_metric_stage($stage_form));
        }

        var variables = CB.parse_metric_variables_specs();
        $.ajax({
            url: URLS.metrics.save,
            method: 'post',
            dataType: 'json',
            crossDomain: true,
            contentType: "application/json",

            data: JSON.stringify({
                internal_id: $form.find(".cb-internal-id-input").val(),
                name: $form.find(".cb-name-input").val(),
                variables: variables,
                stages: stages
            }),

            success: function(result) {
                CB._FORM_CLEAN = true;
                CB.show_popup('OK', 'Metric is saved');
                $form.find(".cb-internal-id-input").val(result.internal_id);
                location.hash = `metric:${result.internal_id || ''}`;
                CB.set_active_menu("#cb-metrics-menu");
            },
            error: CB.process_http_error
        });
    }

    CB.render_metric_error = function(internal_id, error_text) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]');

        $elem.empty();
        $elem.append(
            "<div class='red-168 full-width white-text cb-metric-error-on-dashboard'>" +
            "<span class='cb-bold'>ERROR: </span><span>" + error_text + "</span><br/>" +
            "</div>"
        );
    }

    CB.render_metric_warning = function(internal_id, text) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]');

        $elem.empty();
        $elem.append(
            "<div class='full-width cb-metric-warning-on-dashboard'>" +
            text +
            "</div>"
        );
    }


    CB.add_controls_to_table = function($table) {
        var $ths = $table.find("thead th");

        $ths.find('.cb-table-column-filter').remove();
        $ths.append(
            "<input class='tui-input cb-table-column-filter' " +
            "onchange='CB.filter_table_onchange(this)'/>"
        );
    }

    CB.filter_table_onchange = function(input) {
        var $input = $(input);
        var target_value = $input.val();

        var th = $input.parents('th')[0];
        var $table = $input.parents('table');
        var ths = $table.find("thead th");

        var i, n=0;
        for (i=0; i<ths.length; ++i) {
            if (ths[i] == th) {
                break;
            }
            n += 1;
        }
        
        console.log(n);
        var $trs = $table.find("tr");

        for(i=0; i<$trs.length; ++i) {
            var $tr = $($trs[i]);
            var $tds = $tr.find("td");
            if (!$tds.length) {
                continue;
            }

            var $td = $($tds[n]);
            var value = $td.html();

            if (value.includes(target_value)) {
                $tr.show();
            } else {
                $tr.hide();
            }
        }
    }

    CB.render_metric_table = function(internal_id, rows) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]');

        $elem.empty();
        $elem.addClass('cb-table-metric-presentation');
        $table = $("<table class='tui-table hovered-cyan width-100'>");
        $elem.append($table);

        if (!Array.isArray(rows)) {
            rows = [rows];
        }

        var i;
        for(i=0; i<rows.length; ++i) {
            var item = rows[i];
            if (!isObject(item)) {
                rows[i] = {'value': item};
            }
        }

        var row = rows[0];
        if (!row) {
            $table.html("<div class='cb-metric-warning-on-dashboard'>No data :(</div>");
            return;
        }
        var i, keys = [];
        if (Array.isArray(row)) {
            for (i=0; i<row.length; ++i) {
                keys.push(row[i][0]);
            }
        } else {
            keys = Object.keys(row);
        }

        var i, j;
        var $thead = $("<thead><tr></tr></thead>");
        for(i=0; i<keys.length; ++i) {
            var k = keys[i];
            $thead.append($("<th>" + k + "</th>"));
        }
        $table.append($thead);
        $tbody = $("<tbody></tbody>");
        $table.append($tbody);

        for(i=0; i<rows.length; ++i) {
            var $tr = $("<tr></tr>");
            var row = rows[i];

            if (Array.isArray(row)) {
                for(j=0; j<row.length; ++j) {
                    var column = row[j];
                    var v = column[1];

                    if (isObject(v)) {
                        v =  JSON.stringify(v);
                    }
                    $tr.append("<td>" + v + "</td>");
                }
            } else {
                for(j=0; j<keys.length; ++j) {
                    var key = keys[j];
                    var v = row[key];

                    if (isObject(v)) {
                        v =  JSON.stringify(v);
                    }
                    $tr.append("<td>" + v + "</td>");
                }
            }

            $tbody.append($tr);
        }
        CB.add_controls_to_table($table);
    }

    CB.render_metric_gantt_chart = function(internal_id, rows) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]')
        $slot = $elem.parents(".cb-metric-presentation-block");

        $elem.empty();

        if (!rows.length) {
            CB.render_metric_warning(internal_id, "No data :(");
            return;
        }

        var i;
        for(i=0; i<rows.length; ++i) {
            var row = rows[i];

            if (!row.name) {
                CB.render_metric_error(
                    internal_id,
                    `In line ${i} name is empty.<br>Row: ${JSON.stringify(row)}.`
                );
                return;
            }

            var data = row.data || [];
            for(j=0; j<data.length; ++j) {
                var item = data[j];

                var temp = item.x;
                item.x = item.y;
                item.y = temp;

                if (!item.x.trim()) {
                    CB.render_metric_error(
                        internal_id,
                        `In line ${i} item ${j} has empty x-value.<br>Row: ${JSON.stringify(row)}.`
                    );
                    return;
                }

                if (!isDateValid(item.y[0]) || !isDateValid(item.y[1])) {
                    CB.render_metric_error(
                        internal_id,
                        `In line ${i} item ${j} has invalid datetime as y-value.<br>Row: ${JSON.stringify(row)}.`
                    );
                    return;
                }
            }
        }

        var min_datetime = $("#cb-dashboard-form .cb-datetime-from").val();
        var max_datetime = $("#cb-dashboard-form .cb-datetime-to").val();

        var elem = $elem[0];

        let chart_id = (Math.random() + 1).toString(36).substring(1);

        $elem.attr('chart_id', chart_id);

        var actor_values = {};
        var bar_values = {};
        var i, j;

        for (i=0; i<rows.length; ++i) {
            var row = rows[i];
            row.name = row.name.trim();

            var data = rows[i].data;
            for(j=0; j<data.length; ++j) {
                data[j].x = data[j].x.trim();
            }
        }

        for (i=0; i<rows.length; ++i) {
            var data = rows[i].data;
            for(j=0; j<data.length; ++j) {
                actor_values[data[j].x] = 1;
                data[j].y[0] = new Date(data[j].y[0]).getTime();
                data[j].y[1] = new Date(data[j].y[1]).getTime();
            }
            bar_values[rows[i].name] = 1;
        }
        actor_values = Object.keys(actor_values);
        actor_values.sort();

        bar_values = Object.keys(bar_values);
        bar_values.sort();

        $controls = $slot.find(".cb-controls");
        $controls.empty();

        var $actor_select_container = $("<div class='cb-gantt-actor-select-container'></div>");
        $controls.append($actor_select_container);

        $actor_select_container.append("Choose actor values:<br> ");

        var $actor_select = $(
            "<select multiple class='tui-input' onchange=\"CB.filter_gantt_data('" +
            internal_id +
            "');\"></select>"
        );
        $actor_select.append("<option></option>");

        for (i=0; i<actor_values.length; ++i) {
            var x_value = actor_values[i];
            $actor_select.append("<option>" + x_value + "</option>");
        }

        $actor_select_container.append($actor_select);

        var $bar_select_container = $("<div class='cb-gantt-bar-select-container'></div>");
        $controls.append($bar_select_container);

        $bar_select_container.append("Choose bar values:<br> ");

        var $bar_select = $(
            "<select multiple class='tui-input' onchange=\"CB.filter_gantt_data('" +
            internal_id +
            "');\"></select>"
        );
        $bar_select.append("<option></option>");

        for (i=0; i<bar_values.length; ++i) {
            var y_value = bar_values[i];
            $bar_select.append("<option>" + y_value + "</option>");
        }

        $bar_select_container.append($bar_select);

        var options = {
            chart: {
                id: chart_id,
                type: 'rangeBar',
                fontFamily: 'Lucida Console, monospace'
            },
            plotOptions: {
                bar: {
                    horizontal: true,
                    barHeight: '80%'
                }
            },
            dataLabels: {
                enabled: true,
                formatter: function(val) {
                    var a = moment(val[0])
                    var b = moment(val[1])
                    var diff = b.diff(a, 'days')
                    return diff + (diff > 1 ? ' days' : ' day')
                }
            },
            series: rows,
            stroke: {
                width: 2,
                curve: 'smooth'
            },
            xaxis: {
                type: 'datetime',
                min: new Date(min_datetime).getTime(),
                max: new Date(max_datetime).getTime()
            },
            yaxis: {
                labels: {
                    maxWidth: 400
                }
            },
            legend: {
                show: false,
                position: 'right',
                horizontalAlign: 'right',
                fontFamily: 'Lucida Console, monospace'
            },
            grid: {
                borderColor: '#111',
                strokeDashArray: 7,

                row: {
                      colors: ['#e5e5e5', 'transparent'],
                      opacity: 0.5
                  }, 
                  column: {
                      colors: ['#f8f8f8', 'transparent'],
                  }, 
                  xaxis: {
                    lines: {
                          show: true
                    }
                  }
            }
        };

        var chart = new ApexCharts(elem, options);
        chart.render();
        CB.CHARTS[chart_id] = chart;
        CB.CHARTS_GANTT_DATA[chart_id] = rows;
    }

    CB.render_metric_timeseries_chart = function(internal_id, rows) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]')
        $elem.empty();

        var elem = $elem[0];

        var i;
        var has_points = false;
        for(i=rows.length - 1; i>=0; --i) {
            var row = rows[i];
            if (row[1].length) {
                has_points = true;
                break;
            }
        }

        if (!rows.length || !has_points) {
            CB.render_metric_warning(internal_id, "No data :(");
            return;
        }

        var i;
        for(i=0; i<rows.length; ++i) {
            var row = rows[i];

            if (row.length != 2) {
                CB.render_metric_error(
                    internal_id,
                    `Line ${i} must be array of 2 elements, now length = ${row.length}.` +
                    `<br>Row: ${JSON.stringify(row)}.`
                );
                return;
            }

            if (!isDateValid(row[0])) {
                CB.render_metric_error(
                    internal_id,
                    `Line ${i} has invalid date ${row[0]}.` +
                    `<br>Row: ${JSON.stringify(row)}.`
                );
                return;
            }
            var j;
            for(j=0; j<row[1].length; ++j) {
                if (!row[1][j].label) {
                    CB.render_metric_error(
                        internal_id,
                        `In line ${i} item ${j} has empty label.<br>Row: ${JSON.stringify(row)}.`
                    );
                    return;
                }

                if (isNaN(row[1][j].value)) {
                    CB.render_metric_error(
                        internal_id,
                        `In line ${i} item ${j} has value which is not a number.<br>Row: ${JSON.stringify(row)}.`
                    );
                    return;
                }
            }
        }

        var min_datetime = new Date(rows[0][0]);
        var max_datetime = new Date(rows[rows.length - 1][0]);

        max_datetime.setTime(max_datetime.getTime() + 300 * 1000);
        min_datetime.setTime(min_datetime.getTime() - 300 * 1000);

        var dynamic_options = CB.get_chart_options(internal_id)
        var series = {};
        $.each(rows, function(index) {
            var points = rows[index][1];

            $.each(points, function(_, point) {
                series[point.label || 'unknown'] = {
                    dataPoints: [],
                    showInLegend: true,
                    toolTipContent: "<h1>{name}</h1>{x} --- {y}",
                    type: dynamic_options.type,
                    fillOpacity: dynamic_options.opacity, 
                    name: point.label || 'unknown'
                };
            });
        });

        $.each(rows, function(index) {
            var row = rows[index];
            var datetime = row[0];
            var points = row[1];

            // выставляем дефолтные null;
            $.each(series, function(label, serie) {
                serie.dataPoints.push({x: new Date(datetime), y: 0});
            });

            // перезаписываем дефолтные нули на настоящие значения.
            $.each(points, function(index, point) {
                var s = series[point.label || 'unknown'];
                s.dataPoints[s.dataPoints.length - 1].y = point.value;
            });
        });


        let chart_id = (Math.random() + 1).toString(36).substring(1);

        $elem.attr('chart_id', chart_id);

        var options = {
            colorSet: "greenShades",
            animationEnabled: true,
            backgroundColor: "#e5e5e5",
            data: Object.values(series),
            itemWrap: true,
            itemWidth: 500,
            legend: {
                verticalAlign: "bottom",
                horizontalAlign: "left",
                fontFamily: "Lucida Console, monospace",
                itemWrap: true,
                itemWidth: 500,
                fontWeight: "normal",
                fontSize: 18,

                cursor: "pointer",
        itemmouseover: function(e) {
            e.dataSeries.lineThickness = e.chart.data[e.dataSeriesIndex].lineThickness * 2;
            e.dataSeries.markerSize = e.chart.data[e.dataSeriesIndex].markerSize + 2;
            e.chart.render();
        },
        itemmouseout: function(e) {
            e.dataSeries.lineThickness = e.chart.data[e.dataSeriesIndex].lineThickness / 2;
            e.dataSeries.markerSize = e.chart.data[e.dataSeriesIndex].markerSize - 2;
            e.chart.render();
        },
        itemclick: function (e) {
            if ((typeof (e.dataSeries.visible) === "undefined" || e.dataSeries.visible)) {
                e.dataSeries.visible = false;
            } else {
                e.dataSeries.visible = true;
            }
            e.chart.render();
        }
            },
            axisX: {
                crosshair: {
                    enabled: true,
                    snapToDataPoint: true
                },
                minimum: min_datetime,
                maximum: max_datetime,
                labelFontFamily: "Lucida Console, monospace",
                fontWeight: "normal",
                labelFontSize: 18,
                labelAngle: 0,
                gridThickness: 1,
                gridDashType: "dot",
                gridColor: "#111"
            },
            axisY: {
                crosshair: {
                    enabled: true,
                    snapToDataPoint: true
                },
                gridThickness: 1,
                gridColor: "#111",
                gridDashType: "dot",
                labelFontFamily: "Lucida Console, monospace",
                fontWeight: "normal",
                labelFontSize: 18,
                labelAngle: 0
            },
            toolbar: {
                autoSelected: 'pan',
                show: true
            },
            toolTip: {
                shared: false
            }
        };

        var chart = new CanvasJS.Chart(elem, options);
        chart.render();
        CB.CHARTS[chart_id] = chart;
    }

    CB.CHARTS = {};
    CB.CHARTS_GANTT_DATA = {};

    CB.empty_metric_form = function() {
        $("#cb-metric-form .cb-stage").remove();
        $("#cb-metric-form .cb-internal-id-input").val('');
        $("#cb-metric-form .cb-name-input").val('');
        $("#cb-metric-form-errors").empty();
        $("#cb-metric-form-errors").hide();
        $('#cb-metric-form .cb-terminal-stage-input').empty();
        $('#cb-metric-variables-list .cb-variables-table .cb-metric-variable-row').remove();

        CB.empty_metric_pipelines();
    }

    CB.filter_gantt_data = function(internal_id) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]')
        var chart_id = $elem.attr('chart_id');
        var chart = CB.CHARTS[chart_id];
        var rows = CB.CHARTS_GANTT_DATA[chart_id];

        var $bar_select = $slot.find(".cb-gantt-bar-select-container select");
        var bar_values = $bar_select.val();
        exclude_from_array(bar_values, '');

        var $actor_select = $slot.find(".cb-gantt-actor-select-container select");
        var actor_values = $actor_select.val();
        exclude_from_array(actor_values, '');

        var bar_value__actors = {};
        var actor__bar_values = {};
        for(i=0; i<rows.length; ++i) {
            var row = rows[i];
            var bar_value = row.name;

            var j;
            for(j=0; j<row.data.length; ++j) {
                var actor = row.data[j].x;

                if (!bar_value__actors[bar_value]) {
                    bar_value__actors[bar_value] = [];
                }

                if (!actor__bar_values[actor]) {
                    actor__bar_values[actor] = [];
                }

                bar_value__actors[bar_value].push(actor);
                actor__bar_values[actor].push(bar_value);
            }
        }

        $.each($bar_select.find('option'), function(index, option) {
            $(option).show();
        });

        $.each($actor_select.find('option'), function(index, option) {
            $(option).show();
        });

        $.each(actor_values, function(index) {
            var actor_value = actor_values[index];

            var valid_bar_values = actor__bar_values[actor_value] || [];

            $.each($bar_select.find('option'), function(index, option) {
                var html = $(option).html();

                if (!html || !valid_bar_values || valid_bar_values.includes(html)) {
                    $(option).show();
                } else {
                    $(option).hide();
                }
            });
        });


        $.each(bar_values, function(index) {
            var bar_value = bar_values[index];

            var valid_actor_values = bar_value__actors[bar_value] || [];

            $.each($actor_select.find('option'), function(index, option) {
                var html = $(option).html();

                if (!html || !valid_actor_values || valid_actor_values.includes(html)) {
                    $(option).show();
                } else {
                    $(option).hide();
                }
            });
        });

        var new_rows = [];
        var i, j;
        for(i=0; i<rows.length; ++i) {
            var row = rows[i];
            var name = row.name;
            if (bar_values.length && bar_values.indexOf(name) == -1) {
                continue;
            }

            var new_data = [];
            for(j=0; j<row.data.length; ++j) {
                var item = row.data[j];
                if (actor_values.length && actor_values.indexOf(item.x) == -1) {
                    continue;
                }

                new_data.push(item);
            }

            new_rows.push({
                name: row.name,
                data: new_data
            })
        }


        ApexCharts.exec(chart_id, 'updateOptions', {
            series: new_rows
        }, false, false);


        var min_datetime = $("#cb-dashboard-form .cb-datetime-from").val();
        var max_datetime = $("#cb-dashboard-form .cb-datetime-to").val();
        ApexCharts.exec(chart_id, 'updateOptions', {
            xaxis: {
                min: new Date(min_datetime).getTime(),
                max: new Date(max_datetime).getTime()
            }
        }, false, false);
    }

    CB.parse_metric_variables_specs = function() {
        var trs = $("#cb-metric-variables-list .cb-metric-variable-row");

        var result = [];
        var i;
        for(i=0; i<trs.length; ++i) {
            var tr = trs[i];
            var tds = $(tr).find('td');
            var name = $(tds[0]).find('input').val();
            var type = $(tds[1]).find('select').val();
            result.push({
                name: name,
                type: type,
            })
        }

        return result;
    }

    CB.dry_run_on_click = function(button) {
        var $button = $(button);
        var $datetime_from = $button.parents('.cb-stage').find('.cb-datetime-from');
        var $datetime_to = $button.parents('.cb-stage').find('.cb-datetime-to');
        var $table = $button.parents('.cb-stage').find('.cb-stage-results');
        var $logs = $button.parents('.cb-stage').find('.cb-stage-logs');

        var is_valid = CB.validate_metric_form();
        if (!is_valid) {
            $("#cb-metric-form-errors").show();
            return;
        } else {
            $("#cb-metric-form-errors").hide();
        }

        $form = $("#cb-metric-form");

        var $stage_forms = $form.find('.cb-stage');

        var stages = [];
        for(i=0; i<$stage_forms.length; ++i) {
            var $stage_form = $($stage_forms[i]);
            stages.push(CB.parse_metric_stage($stage_form));
        }

        var $stage_form = $button.parents('.cb-stage');
        var stage = CB.parse_metric_stage($stage_form)

        var variables = CB.parse_variables($stage_form.find('.cb-variables .cb-variable'));
        $.ajax({
            url: URLS.metrics.dry_run,
            method: 'post',
            dataType: 'json',
            crossDomain: true,
            contentType: "application/json",

            data: JSON.stringify({
                stages: stages,
                stage_name: stage.name,
                datetime_from: $datetime_from.val(),
                datetime_to: $datetime_to.val(),
                variables: variables,
            }),

            success: function(response) {
                $table.empty();

                var logs = response.logs || [];
                var i;

                if (logs.length) {
                    $logs.empty();
                } else {
                    $logs.html("No logs...");
                }
                for(i=0; i<logs.length; ++i) {
                    var log = logs[i];
                    var $item = $(
                        `<div class='cb-dry-run-log-item'>` +
                        `<span class='cb-dry-run-log-item-message'><span class='cb-info-level'>INFO</span> ${log.message}</span>` +
                        `<br><br>` +
                        `</div>`
                    );
                    $logs.append($item);
                }

                var error = response.error;
                if (error) {
                    $table.html(`<h1>Error</h1><div>${error}</div>`);
                    return;
                }

                var result = response.result;
                if (!result) {
                    $table.html("<h1>Error</h1><div>Terminal stage did not return any result.</div>");
                    return;
                }

                if (result.error) {
                    $table.html("<h1>Error</h1><div>" + result.error + "</div>");
                    return;
                }

                if (!Array.isArray(result)) {
                    result = [result];
                }

                var i;
                for(i=0; i<result.length; ++i) {
                    var item = result[i];
                    if (!isObject(item)) {
                        result[i] = {'value': item};
                    }
                }

                var row = result[0];
                if (!row) {
                    $table.html("Result: no data");
                    return;
                }
                var i, keys = [];
                if (Array.isArray(row)) {
                    for (i=0; i<row.length; ++i) {
                        keys.push(row[i][0]);
                    }
                } else {
                    keys = Object.keys(row);
                }

                var i, j;
                var $thead = $("<thead><tr></tr></thead>");
                for(i=0; i<keys.length; ++i) {
                    var k = keys[i];
                    $thead.append($("<th>" + k + "</th>"));
                }
                $table.append($thead);
                $tbody = $("<tbody></tbody>");
                $table.append($tbody);

                for(i=0; i<result.length; ++i) {
                    var $tr = $("<tr></tr>");
                    var row = result[i];
                    if (Array.isArray(row)) {
                        for(j=0; j<row.length; ++j) {
                            var column = row[j];
                            var v = column[1];

                            if (isObject(v)) {
                                v =  JSON.stringify(v);
                            }
                            $tr.append("<td>" + v + "</td>");
                        }
                    } else {
                        for(j=0; j<keys.length; ++j) {
                            var key = keys[j];
                            var v = row[key];

                            if (isObject(v)) {
                                v =  JSON.stringify(v);
                            }
                            $tr.append("<td>" + v + "</td>");
                        }
                    }
                    $tbody.append($tr);
                }

                CB.add_controls_to_table($table);
            },
            error: CB.process_http_error
        });

    }

    CB.delete_stage_on_click = function(button) {
        CB._FORM_CLEAN = false;

        var $tabs = $("#cb-metric-form .cb-stage-tabs .tui-tab");
        if ($tabs.length == 1) {
            CB.show_popup("Invalid action", "Metric must have at least one stage");
            return;
        }

        var $button = $(button);
        var $stage = $button.parents('.cb-stage');

        var deleted_name = $stage.find(".cb-stage-name-input").val();
        $stage.remove();

        var $is_terminal_input = $('#cb-metric-form .cb-terminal-stage-input');
        var current_terminal_stage = $is_terminal_input.val();

        var j;
        var $options = $is_terminal_input.find("option");
        for(j=0; j<$options.length; ++j) {
            $option = $($options[j]);
            if ($option.html() == deleted_name) {
                $option.remove();
            }
        }

        if (current_terminal_stage == deleted_name) {
            $is_terminal_input.val('');
        }

        var i;
        var $tabs = $("#cb-metric-form .cb-stage-tabs .tui-tab");
        var number = 1;
        for(i=0; i<$tabs.length; ++i) {
            var $tab = $($tabs[i]);

            if ($tab.attr('data-tab-content') == $stage.attr('id')) {
                $tab.parent("li").remove();
            } else {
                if (number == 1) {
                    var $tab = $($tabs[i]);
                    CB.make_tab_active($tab);
                }
                number += 1;
            }
        }

        var $all_stage_inputs = $(".cb-input-stage-name-input");

        var i;
        for(i=0; i<$all_stage_inputs.length; ++i) {
            var $input = $($all_stage_inputs[i]);
            if ($input.val() == deleted_name) {
                $input.val('');
            }
        }

        CB.actualize_stage_inputs();
    }

    var tabs_counter = 0;

    CB.sync_is_terminal_onchange = function(select) {
        // Будем выводить ошибку вместе автоматического сброса галочки.
        return;
        var inputs = $(select).parents(".content").find(".cb-is-terminal-input");
        var changed_stage = $(select).parents('.cb-stage');

        if ($(select).val() == 'Yes') {
            var i;
            for(i=0; i<inputs.length; ++i) {
                var input = inputs[i];
                var stage = $(input).parents('.cb-stage');
                if ($(stage).attr('id') != $(changed_stage).attr('id')) {
                    $(input).val('No');
                }
            }
        }
    }

    CB.empty_metric_pipelines = function() {
        $('#cb-metric-form .cb-stage-tabs .cb-tabs-header ul').empty();
        $('#cb-metric-form .cb-stage-tabs .content').empty();
        tabs_counter = 0;
    }

    CB.add_new_metric_stage_onclick = function() {
        CB._FORM_CLEAN = false;

        CB.add_metric_pipeline_stage();
        CB.actualize_stage_inputs();
        var block = $(".cb-variables")[0];
        CB.sync_metric_variables($(block));
    }

    CB.add_metric_pipeline_stage = function(stage) {
        tabs_counter += 1;

        if (!stage) {

            var $tabs = $("#cb-metric-form .cb-stage-tabs .cb-tabs-header .tui-tab");
            var j;
            var stage_names = [];
            for(j=0; j<$tabs.length; ++j) {
                var $tab = $($tabs[j]);
                var stage_name = $tab.html();
                stage_names.push(stage_name);
            }
            var new_name;
            while (true) {
                new_name = 'stage-' + get_random_id();
                if (!stage_names.includes(new_name)) {
                    break;
                } 
            }

            stage = {
                is_terminal: false,
                name: new_name,
                inputs: [{

                }],
                action: {}
            }
        }

        var $form = $("#cb-metric-form");
        $form.find('.cb-terminal-stage-input').append(
            "<option>" + stage.name + "</option>"
        );
        if (stage.is_terminal) {
            $form.find('.cb-terminal-stage-input').val(stage.name);
        }

        var $stages = $("#cb-metric-form .cb-stage-tabs");

        var $lis = $stages.find(".cb-tabs-header ul");

        var $li_aa = $lis.find(".tui-tab");
        $li_aa.removeClass("active");

        var content_id = "stage-tab-content-" + tabs_counter;
        var $li = $(
            "<li><a class='tui-tab active'" +
            "data-tab-content='" + content_id + "'>" +
            stage.name + "</a></li>"
        );
        $lis.append($li);
        
        $contents = $stages.find('.cb-stage-tabs-content');

        var $content = $(
            "<div class='cb-stage tui-tab-content' id='" + content_id + "'>"
        );
        $contents.append($content);

        var html = (
            "<button class=\"tui-button red-168 cb-delete-stage\" onclick=\"CB.delete_stage_on_click(this);\">[X]Delete</button>" +
            "<br>"
        );
        $content.append($(html));

        $content.append($(
            "<span>Name................: </span>"
        ));
        $content.append($(
            "<input class='cb-stage-name-input tui-input' value='" + stage.name + "' " +
            "onchange='CB.onchange_stage_input_name(this);' " +
            "/>"
        ));
        $content.append($('<br>'));

        $content.append($(
            "<span class='cb-float-left'>Inputs..............:&nbsp</span>"
        ));

        $inputs = $("<div class='cb-stage-inputs'></div>")
        $content.append($inputs);

        $inputs.append($(
            "<button class=\"tui-button cb-add-new-input\" onclick=\"CB.add_new_stage_input_onclick(this);\">" +
            "+" +
            "</button>"
        ));

        var i;
        for(i=0; i<stage.inputs.length; ++i) {
            CB.add_stage_input(stage.inputs[i], $inputs);
        }

        $content.append("<br/>");


        $content.append($(
            "<span class='cb-float-left'>Action type.........:&nbsp</span>"
        ));

        var $action_type = $(
            "<select class='tui-input cb-action-type-input' " +
            "onchange='CB.update_action_details_inputs(this)'>" +
            "<option>query</option>" +
            "<option>rpc</option>" +
            "<option>python</option>" +
            "</select>"
        );
        $action_type.val(stage.action.action_type || 'query');

        $content.append($action_type);

        $content.append("<br/>");

        $action_details = $(
            "<div class='cb-action-details'></div>"
        );
        $content.append($action_details);

        CB.update_action_details_inputs($action_type, stage);
 
        $content.append("<br/>");

        var html = (
            "<button class=\"tui-button\" onclick=\"CB.dry_run_on_click(this);\">" +
            "Show result of this stage" +
            "</button> " +
            "<br>" +
            "<div class='cb-variables'>" +
            "    <div class='cb-variable'>" +
            "        <input disabled class='tui-input' value='datetime_from'> = " + 
            "        <input class='cb-datetime cb-datetime-from cb-datetime-week-ago' type='datetime-local' />" +
            "        <br/>" +
            "    </div>" +
            "    <div class='cb-variable'>" + 
            "        <input disabled class='tui-input' value='datetime_to'> = " +
            "        <input class='cb-datetime cb-datetime-to cb-datetime-now' type='datetime-local' />" +
            "        <br/>" +
            "    </div>" +
            "</div>" +
            "<br>" +
            "<div class='tui-panel width-100 cb-stage-results-tabs'>" +
            "<div class='tui-tabs width-100'>" +
            "    <ul>" +
            `    <li><a class='tui-tab active' data-tab-content='cb-stage-results-result-${tabs_counter}'>Result</a></li>` +
            `    <li><a class='tui-tab' data-tab-content='cb-stage-results-logs-${tabs_counter}'>Logs</a></li>` +
            "    </ul>" +
            "</div>" +
            "<div class='content'>" +
            `    <div id='cb-stage-results-result-${tabs_counter}' class='tui-tab-content'> ` +
            "        <table class='tui-table hovered-cyan width-100 cb-stage-results'><tbody><tr><td>No results yet...</td></tr></tbody></table>" +
            "    </div>" +
            `    <div id='cb-stage-results-logs-${tabs_counter}' class='tui-tab-content width-100 cb-stage-logs'>` +
            "No logs yet...</div>" +
            "</div>" +
            "</div>"
        );
        $content.append($(html));

        CB.make_tab_active($li.find('a'));
        CB.make_tab_active($content.find('.cb-stage-results-tabs .tui-tab')[0]);
        initiate_datetime_inputs();
    }

    CB.actualize_stage_inputs = function() {
        var $all_stage_inputs = $("#cb-metric-form .cb-input-stage-name-input");
        var $tabs = $("#cb-metric-form .cb-stage-tabs .cb-tabs-header .tui-tab");
        var i;
        var j;

        var valid_stage_names = [""];
        for(j=0; j<$tabs.length; ++j) {
            var $tab = $($tabs[j]);
            valid_stage_names.push($tab.html());
        }

        for(i=0; i<$all_stage_inputs.length; ++i) {
            var $input = $($all_stage_inputs[i]);
            var current_stage_name = $input.parents(".cb-stage").find(".cb-stage-name-input").val();

            var $options = $input.find('option');
            var option_values = [];

            for(j=0; j<$options.length; ++j) {
                var $option = $($options[j]);
                var option_value = $option.html();

                if (!valid_stage_names.includes(option_value)) {
                    if (option_value == $input.val()) {
                        $input.val('');
                    }
                    $option.remove();
                } else {
                    option_values.push(option_value);
                }
            }

            for(j=0; j<valid_stage_names.length; ++j) {
                var stage_name = valid_stage_names[j];
                
                if (stage_name != current_stage_name && !option_values.includes(stage_name)) {
                    $input.append("<option>" + stage_name + "</option>");
                }
            }

        }
    }

    CB.get_source_status_class = function(status) {
        status = status.toUpperCase();
        if (status == 'INDEXING') {
            return 'cb-status-provisioning';
        } else if (status == 'ERROR') {
            return 'cb-status-error';
        } else if (status == 'INDEXED') {
            return 'cb-status-indexed';
        }
    }

    CB.onchange_stage_input_name = function(input) {
        var $input = $(input);

        var stage_name = $input.val();

        var $content = $input.parents(".cb-stage");
        var content_id = $content.attr('id');

        var tab_header = $("[data-tab-content=" + content_id + "]");
        var prev_stage_name = tab_header.html();

        var $tabs = $("#cb-metric-form .cb-stage-tabs .tui-tab");
        var j;
        for (j=0; j<$tabs.length; ++j) {
            var $tab = $($tabs[j]);
            if ($tab.html() == stage_name) {
                // Такое имя уже есть. Не даём поменять.
                $input.val(prev_stage_name);
                CB.show_popup("Invalid action", "Stage name must be unique.");
                return;
            }
        }

        if (!stage_name.trim()) {
            $input.val(prev_stage_name);
            CB.show_popup("Invalid action", "Stage name cannot be empty.");
            return;
        }

        tab_header.html(stage_name);

        var $is_terminal_input = $('#cb-metric-form .cb-terminal-stage-input');
        var options = $is_terminal_input.find("option");
        var i;
        for (i=0; i<options.length; ++i) {
            var option = $(options[i]);

            if (option.html() == prev_stage_name) {
                option.html(stage_name);
            }
        }

        if ($is_terminal_input.val() == prev_stage_name) {
            $is_terminal_input.val(stage_name);
        }

        var $stage_input = $('#cb-metric-form .cb-input-stage-name-input');
        var options = $stage_input.find("option");
        var i;
        for (i=0; i<options.length; ++i) {
            var option = $(options[i]);

            if (option.html() == prev_stage_name) {
                option.html(stage_name);
            }
        }

        if ($stage_input.val() == prev_stage_name) {
            $stage_input.val(stage_name);
        }
    }

    CB.empty_redmine_form = function() {
        $("#cb-redmine-form .cb-internal-id-input").val('');
        $("#cb-redmine-form .cb-name-input").val('');
        $("#cb-redmine-form .cb-url-input").val('');
        $("#cb-redmine-form .cb-login-input").val('');
        $("#cb-redmine-form .cb-password-input").val('');
        $("#cb-redmine-form .cb-indexed-at-input").val('');
        $("#cb-redmine-form .cb-index-period-input").val(60);
        $("#cb-redmine-form .cb-index-status-input").val('');
        $("#cb-redmine-form-errors").empty();
        $("#cb-redmine-form-errors").hide();
        $("#cb-redmine-indexify-log").empty();
        $("#cb-redmine-form .cb-projects-input").val('');
        $("#cb-redmine-custom-fields tbody").html('');
    }

    CB.empty_jira_form = function() {
        $("#cb-jira-form .cb-internal-id-input").val('');
        $("#cb-jira-form .cb-name-input").val('');
        $("#cb-jira-form .cb-url-input").val('');
        $("#cb-jira-form .cb-login-input").val('');
        $("#cb-jira-form .cb-password-input").val('');
        $("#cb-jira-form .cb-indexed-at-input").val('');
        $("#cb-jira-form .cb-index-period-input").val(60);
        $("#cb-jira-form .cb-index-status-input").val('');
        $("#cb-jira-form-errors").empty();
        $("#cb-jira-form-errors").hide();
        $("#cb-jira-indexify-log").empty();
        $("#cb-jira-form .cb-projects-input").val('');
        $("#cb-jira-custom-fields tbody").html('');
    }

    CB.is_valid_url = function(string) {
        let url;

        try {
          url = new URL(string);
        } catch (_) {
          return false;
        }

        return url.protocol === "http:" || url.protocol === "https:";
    }

    CB.validate_redmine_form = function(payload) {
        var $errors = $("#cb-redmine-form-errors");
        $errors.empty();

        if (!payload.name) {
            CB.add_form_error($errors, "Name is empty.");
            return false;
        }

        if (!payload.url) {
            CB.add_form_error($errors, "URL is empty.");
            return false;
        }

        if (!CB.is_valid_url(payload.url)) {
            CB.add_form_error($errors, "URL is not a valid URL.");
            return false;
        }

        if (payload.auth_method == 'basic') {
            if (!payload.login) {
                CB.add_form_error($errors, "Login is empty.");
                return false;
               }

            if (!payload.password) {
                CB.add_form_error($errors, "Password is empty.");
                return false;
            }
        } else {
            if (!payload.token) {
                CB.add_form_error($errors, "Personal token is empty.");
                return false;
            }

        }

        if (!payload.projects.length) {
            CB.add_form_error($errors, "Projects are not set.");
            return false;
        }

        return true;
    }

    CB.validate_jira_form = function(payload) {
        var $errors = $("#cb-jira-form-errors");
        $errors.empty();

        if (!payload.name) {
            CB.add_form_error($errors, "Name is empty.");
            return false;
        }

        if (!payload.url) {
            CB.add_form_error($errors, "URL is empty.");
            return false;
        }

        if (!CB.is_valid_url(payload.url)) {
            CB.add_form_error($errors, "URL is not a valid URL.");
            return false;
        }

        if (payload.auth_method == 'basic') {
            if (!payload.login) {
                CB.add_form_error($errors, "Login is empty.");
                return false;
               }

            if (!payload.password) {
                CB.add_form_error($errors, "Password is empty.");
                return false;
            }
        } else {
            if (!payload.token) {
                CB.add_form_error($errors, "Personal token is empty.");
                return false;
            }

        }

        if (!payload.projects.length) {
            CB.add_form_error($errors, "Projects are not set.");
            return false;
        }

        var i;
        for(i=0; i<payload.custom_fields.length; ++i) {
            var row = payload.custom_fields[i];
            if (!row.source) {
                CB.add_form_error($errors, "Source field is empty.");
                return false;
            }

            if (!row.target) {
                CB.add_form_error($errors, "Target field is empty.");
                return false;
            }

            if (!row.type) {
                CB.add_form_error($errors, "Type of custom field is empty.");
                return false;
            }
        }

        return true;
    }

    CB.save_redmine = function() {
        $form = $("#cb-redmine-form");
        var projects = [];
        var raw_projects = $form.find(".cb-projects-input").val().split(/\s+/);
        var i;
        for(i=0; i<raw_projects.length; ++i) {
            var project_name = raw_projects[i].trim();
            if (project_name) {
                projects.push(project_name);
            }
        }

        var payload = {
            internal_id: $form.find(".cb-internal-id-input").val(),
            name: $form.find('.cb-name-input').val().trim(),
            url: $form.find(".cb-url-input").val().trim(),
            auth_method: $form.find(".cb-auth-method-input").val().trim(),
            token: $form.find(".cb-token-input").val().trim(),
            login: $form.find(".cb-login-input").val().trim(),
            password: $form.find(".cb-password-input").val().trim(),
            projects: projects
        }

        var is_valid = CB.validate_redmine_form(payload);
        if (!is_valid) {
            $("#cb-redmine-form-errors").show();
            return;
        } else {
            $("#cb-redmine-form-errors").hide();
        }

        $.ajax({
            url: URLS.redmines.save,
            method: 'post',
            dataType: 'json',
            crossDomain: true,
            contentType: "application/json",

            data: JSON.stringify(payload),

            success: function(result) {
                CB._FORM_CLEAN = true;
                CB.show_popup('OK', 'Source is saved');
                CB.render_redmine_form(result.internal_id);
                location.hash = `redmine:${result.internal_id || ''}`;
                CB.set_active_menu("#cb-sources-menu");
            },
            error: CB.process_http_error
        });
    }

    CB.save_jira = function() {
        $form = $("#cb-jira-form");
        var projects = [];
        var raw_projects = $form.find(".cb-projects-input").val().split(/\s+/);
        var i;
        for(i=0; i<raw_projects.length; ++i) {
            var project_name = raw_projects[i].trim();
            if (project_name) {
                projects.push(project_name);
            }
        }

        var custom_fields_trs = $("#cb-jira-custom-fields tr");
        custom_fields = [];
        // пропускаем заголовки
        for(i=1; i<custom_fields_trs.length; ++i) {
            var tr = $(custom_fields_trs[i]);
            custom_fields.push({
                source: tr.find('.cb-custom-field-source-name').val(),
                target: tr.find('.cb-custom-field-target-name').val(),
                type: tr.find('.cb-custom-field-type').val()
            });
        }

        var payload = {
            internal_id: $form.find(".cb-internal-id-input").val(),
            name: $form.find('.cb-name-input').val().trim(),
            url: $form.find(".cb-url-input").val().trim(),
            auth_method: $form.find(".cb-auth-method-input").val().trim(),
            token: $form.find(".cb-token-input").val().trim(),
            login: $form.find(".cb-login-input").val().trim(),
            password: $form.find(".cb-password-input").val().trim(),
            projects: projects,
            custom_fields: custom_fields
        }

        var is_valid = CB.validate_jira_form(payload);
        if (!is_valid) {
            $("#cb-jira-form-errors").show();
            return;
        } else {
            $("#cb-jira-form-errors").hide();
        }

        $.ajax({
            url: URLS.jiras.save,
            method: 'post',
            dataType: 'json',
            crossDomain: true,
            contentType: "application/json",

            data: JSON.stringify(payload),

            success: function(result) {
                CB._FORM_CLEAN = true;
                CB.show_popup('OK', 'Source is saved');
                CB.render_jira_form(result.internal_id);
                location.hash = `jira:${result.internal_id || ''}`;
                CB.set_active_menu("#cb-sources-menu");
            },
            error: CB.process_http_error
        });
    }

    CB.add_jira_custom_fields_onclick = function() {
        CB._FORM_CLEAN = false;

        CB.add_jira_custom_fields();
        $('#cb-empty-custom-fields-list').hide();
    }

    CB.delete_jira_custom_field_onclick = function(button) {
        CB._FORM_CLEAN = false;
        var $tbody = $(button).parents('tbody');

        $(button).parents('tr').remove();
        if ($tbody.find('tr').length == 0) {
            $('#cb-empty-custom-fields-list').show();
        }
    }

    CB.add_jira_custom_fields = function(source_value, target_value, type_value) {
        var container = $(
            "<div class='cb-container'></div>"
        );
        var select = $(
            "<input class='tui-input cb-custom-field-source-name cb-100'/>"
        );
        select.val(source_value || '');

        var name = $(
            "<input class='tui-input cb-custom-field-target-name cb-100'/>"
        );
        name.val(target_value || '');

        var type = $(
            "<select class='tui-input cb-custom-field-type cb-100'>" +
            "<option>text</option>" +
            "<option>number</option>" +
            "<option>datetime</option>" +
            "</select>"
        );
        type.val(type_value || 'text');

        var delete_button = $(
            "<button class='tui-button cb-100' " +
            "onclick='CB.delete_jira_custom_field_onclick(this);'>" +
            "Delete</button>"
        );
        var tr = $("<tr></tr>")
        var td1 = $('<td></td>');
        td1.append(select);
        tr.append(td1);

        var td2 = $('<td></td>');
        td2.append(name);
        tr.append(td2);

        var td3 = $('<td></td>');
        td3.append(type);
        tr.append(td3);

        var td4 = $('<td></td>');
        td4.append(delete_button);
        tr.append(td4);

        $("#cb-jira-custom-fields").append(tr);
    }

    CB.onchange_jira_auth_method = function() {
        var $form = $("#cb-jira-form");
        var method = $form.find(".cb-auth-method-input").val();

        if (method == 'basic') {
            $form.find(".cb-auth-method-basic-section").show();
            $form.find(".cb-auth-method-token-section").hide();
        } else {
            $form.find(".cb-auth-method-basic-section").hide();
            $form.find(".cb-auth-method-token-section").show();
        }
    }

    CB.render_jira_form = function(internal_id, preserve_before_update) {
        var clean = CB.confirm_form_close(function() {
            CB.render_jira_form(internal_id, preserve_before_update);
        });

        if (!clean) {
            return;
        }

        location.hash = `jira:${internal_id || ''}`;
        CB.set_active_menu("#cb-sources-menu");

        var $form = $("#cb-jira-form");
        var $legend = $("#cb-jira-form > legend");

        if (!preserve_before_update) {
            CB.empty_jira_form();
        }

        var _finish_form = function() {
            CB.hide_all_innerblocks();
            var $panel = $("#cb-jira-form");

            CB.onchange_jira_auth_method();
            $panel.show(100);
        }

        if (internal_id) {
            $.ajax({
                url: URLS.jiras.list + "/" + internal_id,

                dataType: 'json',
                crossDomain: true,

                success: function(jira) {
                    $legend.text("Jira [" + jira.name + "]");
                    $form.find(".cb-internal-id-input").val(jira.internal_id);
                    $form.find(".cb-name-input").val(jira.name);
                    $form.find(".cb-url-input").val(jira.url);
                    $form.find(".cb-login-input").val(jira.login);
                    $form.find(".cb-password-input").val(jira.password);
                    $form.find(".cb-auth-method-input").val(jira.auth_method || 'basic');
                    $form.find(".cb-token-input").val(jira.token);
                    $form.find(".cb-projects-input").val(
                        (jira.projects || []).join("\n")
                    );
                    $form.find(".cb-index-period-input").val(jira.index_period);
                    $form.find(".cb-index-status-input").val(jira.status);

                    $form.find(".cb-indexed-at-input").val(
                        iso_to_human(jira.indexed_at, 'UTC')
                    );

                    var i;
                    for (i=0; i<jira.logs.length; ++i) {
                        CB.add_jira_log(jira.logs[i]);
                    }

                    if (!jira.logs.length) {
                        var $log_container = $("#cb-jira-indexify-log");
                        $log_container.html("No logs yet...");
                    }
                    var custom_fields = jira.custom_fields || [];
                    $("#cb-jira-custom-fields tbody").html('');
                    for(i=0; i<custom_fields.length; ++i) {
                        var custom_field = jira.custom_fields[i];
                        CB.add_jira_custom_fields(
                            custom_field.source,
                            custom_field.target,
                            custom_field.type
                        );
                        $('#cb-empty-custom-fields-list').hide();
                    }

                    _finish_form();
                },
                error: CB.process_http_error
            });

        } else {
            $legend.html("Creating a new jira source");
            _finish_form();
        }
    }


    CB.get_log_level_class = function(level) {
        level = level.toUpperCase();
        if (level == 'INFO') {
            return 'cb-info-level';
        } else if (level == 'ERROR') {
            return 'cb-exception-level';
        }
    }

    CB.add_redmine_log = function(log, prepend) {
        var $log_container = $("#cb-redmine-indexify-log");

        var $item = $(
            `<div class='cb-redmine-log-item'>` +
            `<span class='cb-redmine-log-item-created-at'>${iso_to_human(log.created_at)}</span>` +
            `<span class='cb-redmine-log-item-level ${CB.get_log_level_class(log.level)}'>${log.level}</span>` +
            `<span class='cb-redmine-log-item-message'></span>` +
            `<br>` +
            `</div>`
        );
        // to prevent injections
        $item.find(".cb-redmine-log-item-message").text(log.message);

        if (prepend) {
            $log_container.prepend($item);
        } else {
            $log_container.append($item);
        }
    }


    CB.add_jira_log = function(log, prepend) {
        var $log_container = $("#cb-jira-indexify-log");

        var $item = $(
            `<div class='cb-jira-log-item'>` +
            `<span class='cb-jira-log-item-created-at'>${iso_to_human(log.created_at)}</span>` +
            `<span class='cb-jira-log-item-level ${CB.get_log_level_class(log.level)}'>${log.level}</span>` +
            `<span class='cb-jira-log-item-message'></span>` +
            `<br>` +
            `</div>`
        );
        // to prevent injections
        $item.find(".cb-jira-log-item-message").text(log.message);

        if (prepend) {
            $log_container.prepend($item);
        } else {
            $log_container.append($item);
        }
    }

    CB.empty_dashboard_form = function() {
        var $form = $("#cb-dashboard-form");
        $form.find(".cb-internal-id-input").val('');
        $form.find(".cb-name-input").val('');

        var $container = $("#cb-dashboard-form .cb-metrics-presentation-container");
        $container.empty();

        var variables = $('#cb-dashboard-form .cb-variable');
        for(i=3; i<variables.length; ++i) {
            variables[i].remove();
        }
    }

    CB.extend_timeline_left = function() {
        var new_base_dt = new Date(CB.PLANNING.datetime_from);
        new_base_dt.setDate(new_base_dt.getDate() - 15);
        CB.PLANNING.datetime_from = new_base_dt;
        CB.PLANNING.days_qty = CB.PLANNING.days_qty + 15;

        CB.PLANNING.render_timeline();
    }

    CB.validate_planning_form = function() {
        var $form = $("#cb-planning-form");
        var $errors = $('#cb-planning-form-errors'); 
        $errors.empty();

        var name = $form.find(".cb-name-input").val();
        if (!name) {
            CB.add_planning_error("Name is empty.");
            return false;
        }

        var payload = {
            internal_id: $form.find(".cb-internal-id-input").val(),
            name: $form.find(".cb-name-input").val(),
            velocity_metric_internal_id: $form.find(".cb-metric-input").val(),
            velocity_period: $form.find(".cb-period-input").val(),
            issue_size_field: $form.find(".cb-issue-size-field-input").val(),
            terminal_states: $form.find(".cb-terminal-states-input").val().split('\n').filter((word) => word.length > 0),
            default_issue_size: $form.find(".cb-default-issue-size-input").val()
        }

        if (!payload.velocity_metric_internal_id) {
            CB.add_planning_error("Velocity metric is not chosen.");
            return false;
        }

        if (!payload.velocity_period) {
            CB.add_planning_error("Velocity period is empty.");
            return false;
        }

        if (!payload.issue_size_field) {
            CB.add_planning_error("Issue size field is empty.");
            return false;
        }

        if (!payload.default_issue_size) {
            CB.add_planning_error("Default issue size is empty.");
            return false;
        }

        if (!payload.terminal_states.length) {
            CB.add_planning_error("Terminal states are not set.");
            return false;
        }

        return true;
    }

    CB.add_planning_error = function(error_text) {
        var $errors = $('#cb-planning-form-errors'); 

        var error = "<span class='cb-bold'>ERROR: </span><span>" + error_text + "</span><br/>"; 
        $errors.append(error);
    }

    CB.save_planning = function() {
        var $form = $("#cb-planning-form");

        var i, j, assigned_issues = {};
        var employees = CB.PLANNING.employees;
        for(i=0; i<employees.length; ++i) {
            var emp = employees[i].name;
            var issues = CB.PLANNING.issues[emp] || [];

            assigned_issues[emp] = [];
            for(j=0; j<issues.length; ++j) {
                var issue = issues[j];

                assigned_issues[emp].push({
                    key: issue.issue.key,
                    internal_id: issue.issue.internal_id,
                });
            }
        }

        var i, day_offs = [];
        var trs = $("#cb-day-offs-chart >tbody >tr");
        for(i=0; i<trs.length; ++i) {
            var tr = trs[i];
            var tds = $(tr).find(">td");
            var employee = $(tds[0]).html();
            if (employee == 'All') {
                employee = '';
            } else {
                employee = employee.substring(''.length);
            }
            day_offs.push({
                employee: employee,
                date_from: $(tds[1]).html(),
                date_to: $(tds[2]).html()
            })
        }

        var payload = {
            internal_id: $form.find(".cb-internal-id-input").val(),
            name: $form.find(".cb-name-input").val(),
            velocity_metric_internal_id: $form.find(".cb-metric-input").val(),
            velocity_period: $form.find(".cb-period-input").val(),
            issue_size_field: $form.find(".cb-issue-size-field-input").val(),
            terminal_states: $form.find(".cb-terminal-states-input").val().split('\n').filter((word) => word.length > 0),
            default_issue_size: $form.find(".cb-default-issue-size-input").val(),
            assigned_issues: assigned_issues,
            day_offs: day_offs
        }

        var is_valid = CB.validate_planning_form(payload);
        if (!is_valid) {
            $("#cb-planning-form-errors").show();
            return;
        } else {
            $("#cb-planning-form-errors").hide();
        }

        $.ajax({
            url: URLS.planning.save,
            method: 'post',
            dataType: 'json',
            crossDomain: true,
            contentType: "application/json",

            data: JSON.stringify(payload),

            success: function(result) {
                CB._FORM_CLEAN = true;

                var self = CB.PLANNING;

                CB.show_popup('OK', 'Planning is saved');
                $form.find(".cb-internal-id-input").val(result.internal_id);
                location.hash = `planning:${result.internal_id || ''}`;
                CB.set_active_menu("#cb-plannings-menu");

                var key, payload = [];
                for(issue_internal_id in self.done_percents) {
                    var change = self.done_percents[issue_internal_id];
                    payload.push({
                        issue_internal_id: issue_internal_id,
                        planning_internal_id: result.internal_id,
                        value: change.value,
                        changed_at: change.changed_at
                    });
                }

                if (payload.length) {
                    $.ajax({
                        url: URLS.planning.save_done_percents,
                        method: 'post',
                        dataType: 'json',
                        crossDomain: true,
                        contentType: "application/json",

                        data: JSON.stringify(payload),

                        success: function(result) {
                            CB.PLANNING.done_percents = {};
                        },
                        error: CB.process_http_error
                    });
                }
            },
            error: CB.process_http_error
        });

    }

    CB.delete_planning_onclick = function() {
        var popup = $("#cb-delete-planning-popup");
        popup.show();
    }

    CB.delete_planning = function() {
        if (!CB.PLANNING.internal_id) {
            return;
        }

        var popup = $("#cb-delete-planning-popup");
        popup.hide();

        $form = $("#cb-planning-form");
        var internal_id = $form.find(".cb-internal-id-input").val();

        if (internal_id) {
            $.ajax({
                url: URLS.planning.delete + "/" + internal_id,
                method: 'delete',
                crossDomain: true,

                success: function(result) {
                    CB.show_popup('OK', 'Planning is deleted');
                    CB.render_plannings_table();
                },
                error: CB.process_http_error
            });
        } else {
            CB.show_popup('OK', 'Planning is deleted');
            CB.render_plannings_table();
        }
    }

    CB.utcnow = function() {
        return new Date();
    }

    CB.extend_timeline_right = function() {
        CB.PLANNING.days_qty = CB.PLANNING.days_qty + 15;
        CB.PLANNING.render_timeline();

    }

    CB.PLANNING = {
        employees: [],

        internal_id: '',
        issue_size_field: 'custom_fields.storypoints',
        default_issue_size: 1,
        days_qty: 15,
        datetime_from: null,
        datetime_to: null,
        days: [],
        months: [],
        issues: {},
        trs: {},
        terminal_states: [
            'Closed'
        ],
        done_percents: {},
        history: {},  // employee: [{}, {}, {'key': 1, summary: 2}]
        day_offs: [],

        clear: function() {
            CB.PLANNING.internal_id = '';
            CB.PLANNING.employees = [];
            CB.PLANNING.issues = {};
            CB.PLANNING.issue_size_field = 'custom_fields.storypoints';
            CB.PLANNING.default_issue_size = 1;
            CB.PLANNING.terminal_states = [
                'Closed'
            ];
            CB.PLANNING.day_offs = [];
            this.history = {};
            this.done_percents = {};
        },

        get_velocities_chart: function() {
            return $("#cb-velocities-chart");
        },

        get_assigned_issues_chart: function() {
            return $("#cb-assigned-issues-chart");
        },

        get_chart: function() {
            return $("#cb-timeline-chart");
        },

        calculate_issue_size: function(issue) {
            if (!this.issue_size_field) {
                return this.default_issue_size;
            }

            var paths = this.issue_size_field.split(".");
            var i;

            var value = issue;
            for (i=0; i<paths.length; ++i) {
                value = value[paths[i]];
            }
            return value || this.default_issue_size;
        },

        calculate_duration: function(issue, velocity, done_percent) {
            var size = this.calculate_issue_size(issue);

            return (100 - done_percent) / 100 * (size / velocity);
        },

        calculate_durations: function() {
            var i;
            for(i=0; i<this.employees.length; ++i) {
                var employee = this.employees[i].name;
                var velocity = this.employees[i].velocity;

                var issues = this.issues[employee] || [];

                var j;
                for(j=0; j<issues.length; ++j) {
                    var issue = issues[j];

                    issue.duration = this.calculate_duration(
                        issue.issue, velocity, issue.done_percent,
                    );
                    issue.size = this.calculate_issue_size(issue.issue);
                }
            }
        },
            
        show_or_hide_empty_warnings: function() {
            var chart = this.get_assigned_issues_chart();
            var issues_lists = $(chart).find('.cb-assigned-issues-list');

            var i;
            for(i=0; i<issues_lists.length; ++i) {
                var list = issues_lists[i];
                var warning = $(list).parent('.cb-employee-row').find('>.cb-empty-warning');

                if (!$(list).find('>tbody').find(">tr").length) {
                    $(list).hide();
                    $(warning).show();
                } else {
                    $(list).show();
                    $(warning).hide();
                }
            }
        },
            
        is_finished: function(issue) {
            var state;
            if (issue.issue) {
                state = issue.issue.status.name;
            } else {
                state = issue.status.name;
            }
            return contains(this.terminal_states, state);
        },

        add_issue_to_assigned_list: function(issues_list, issue) {
            $(issues_list).find('thead').show();
            var klass = '';
            if (this.is_finished(issue)) {
                klass += " cb-finished";
            }
            var done_percent;
            if (this.is_finished(issue)) {
                done_percent = 100;
            } else {
                done_percent = Math.ceil(issue.done_percent);
            }
            var formated_done_percent = this.format_done_percent(done_percent);
            var html = (
                `<tr class='cb-assigned-issues-item ${klass}'>` +
                `<td class='cb-delete-button-td'><button onclick="CB.PLANNING.ondelete_assigned_issue(this);">X</button></td>` +
                `<td class='cb-issue-key'>${issue.issue.key} ` +
                `<span class='cb-issue-internal_id' style='display: none;'>` +
                `${issue.issue.internal_id}</span>` +
                `</td>` +
                `<td>${issue.issue.jira.name}</td>` +
                `<td>${issue.issue.status.name}</td>` +
                `<td>${issue.issue.summary}</td>` +
                `<td>${issue.size}</td>` +
                `<td class='cb-ends-in'></td>` +
                `<td><input onchange="CB.PLANNING.onchange_done_percent(this);" ` +
                `type="number" class='cb-done' min="0" ` +
                `value="${Math.min(formated_done_percent, 100)}">` +
                `${(done_percent > 100)? "<span class='cb-tooltip cb-overdue-progress'>!<span class='cb-tooltiptext'>Planned time has expired... </span></span>": ""}</td>` +
                `<td class='cb-overdue ${issue.overdue > 0? "cb-has-overdue": ""}'>${issue.overdue}</td>` +
                `<td><button onclick='CB.PLANNING.onclick_move_issue_up(this);' class='cb-microbutton'>&#8593;</button>` +
                `<button onclick='CB.PLANNING.onclick_move_issue_down(this);' class='cb-microbutton'>&#8595;</button></td>` +
                `</tr>`
            );
            $(issues_list).find('tbody').append($(html));
        },

        format_done_percent: function(value) {
            return Math.floor(value / 5) * 5;
        },

        onchange_issue_size: function(input) {
            this.issue_size_field = $(input).val();
            this.calculate_durations();
            this.render_assigned_issues();
            this.render_timeline();
        },

        onchange_default_issue_size: function(input) {
            this.default_issue_size = $(input).val();
            this.calculate_durations();
            this.render_assigned_issues();
            this.render_timeline();
        },

        onchange_terminal_states: function(input) {
            this.terminal_states = $(input).val().split('\n');
            this.calculate_durations();
            this.render_assigned_issues();
            this.render_timeline();
        },

        onchange_metric: function(input) {
            CB._FORM_CLEAN = false;

            var $form = $("#cb-planning-form");
            var metric_internal_id = $form.find(".cb-metric-input").val();
            if (metric_internal_id) {
                $("#cb-change-velocity-metric-link a").attr(
                    "href",
                    `/#metric:${metric_internal_id}`
                );
            }
            CB.PLANNING.actualize_employees();
        },

        onchange_period: function(input) {
            CB.PLANNING.actualize_employees();
        },

        configure_elements_if_no_employees: function() {
            $('.cb-no-velocity-metric').show();
            $("#cb-velocities-chart").hide();
            $("#cb-timeline-chart").hide();
            $("#cb-extend-timeline-left").hide();
            $("#cb-extend-timeline-right").hide();
        },

        configure_elements_if_employees: function() {
            $('.cb-no-velocity-metric').hide();
            $("#cb-velocities-chart").show();

            $("#cb-timeline-chart").show();
            $("#cb-extend-timeline-left").show();
            $("#cb-extend-timeline-right").show();
        },

        actualize_employees: function() {
            var self = this;
            var $form = $("#cb-planning-form");
            var metric_internal_id = $form.find(".cb-metric-input").val();
            var period = $form.find(".cb-period-input").val();

            if (!metric_internal_id) {
                this.configure_elements_if_no_employees();
                return;
            }

            var now = new Date();
            var not_now = new Date();
            not_now.setDate(now.getDate() - parseInt(period));
            var payload = {
                internal_id: metric_internal_id,
                variables: {},
                datetime_from: not_now,
                datetime_to: now,
                period: period,
                metric_type: 'table'
            }
            $.ajax({
                url: URLS.metrics.run,
                method: 'post',
                dataType: 'json', 
                crossDomain: true,
                contentType: "application/json",

                data: JSON.stringify(payload),
                success: function(reply) {
                    self.employees = reply.data;                    
                    if (!self.employees.length) {
                        self.configure_elements_if_no_employees();
                    } else {
                        self.configure_elements_if_employees();
                        self.render();
                    }

                }
            });
        },

        onclick_move_issue_up: function(button) {
            CB._FORM_CLEAN = false;
            var employee = $(button).parents(".cb-employee-row").attr('cb-employee');

            var $row = $(button).parents(".cb-assigned-issues-item");

            var internal_id = $row.find(".cb-issue-internal_id").html();
            var issues = this.issues[employee];
            var pos;
            for(i=0; i<issues.length; ++i) {
                if (issues[i].internal_id == internal_id) {
                    pos = i;
                    break;
                }
            }
            if (pos > 0) {
                swap_in_array(issues, pos, pos - 1);
                var $before = $row.prev();
                $row.insertBefore($before);
                this.render_issues(employee);
                this.actualize_timings(employee);
            }
        },

        onclick_move_issue_down: function(button) {
            CB._FORM_CLEAN = false;

            var employee = $(button).parents(".cb-employee-row").attr('cb-employee');

            var $row = $(button).parents(".cb-assigned-issues-item");

            var internal_id = $row.find(".cb-issue-internal_id").html();
            var issues = this.issues[employee];
            var pos;
            for(i=0; i<issues.length; ++i) {
                if (issues[i].internal_id == internal_id) {
                    pos = i;
                    break;
                }
            }
            if (pos < issues.length - 1) {
                swap_in_array(issues, pos, pos + 1);

                var $after = $row.next();
                $row.insertAfter($after);
                this.render_issues(employee);
                this.actualize_timings(employee);
            }
        },

        ondelete_assigned_issue: function(button) {
            var employee = $(button).parents(".cb-employee-row").attr('cb-employee');
            var internal_id = $(button).parents(".cb-assigned-issues-item").find(".cb-issue-internal_id").html();

            var assigned_row = $(button).parents(".cb-assigned-issues-item");
            assigned_row.remove();

            var issues = this.issues[employee] || [];
            this.issues[employee] = issues;

            var i;
            for(i=0; i<issues.length; ++i) {
                var issue = issues[i];

                if(issue.internal_id == internal_id) {
                    this.issues[employee].splice(i, 1);       
                }
            }
            this.render_all_issues();
            this.render_history();
            this.show_or_hide_empty_warnings();
        },

        onchange_done_percent: function(input) {
            var val = $(input).val();

            var internal_id = $(input).parents(".cb-assigned-issues-item").find(".cb-issue-internal_id").html();
            this.done_percents[internal_id] = {
                value: val,
                changed_at: CB.utcnow() 
            };
            $(input).parents(".cb-assigned-issues-item").find(".cb-overdue").html('0');

            var changed = false;
            for(i=0; i<this.employees.length; ++i) {
                var employee = this.employees[i].name;
                issues = this.issues[employee] || [];

                for(j=0; j<issues.length; ++j) {
                    issue = issues[j];
                    if (issue.issue.internal_id == internal_id) {
                        issue.done_percent = val;
                        issue.overdue = 0;
                        changed = true;
                    }
                }
            }

            if (changed) {
                this.calculate_durations();
                this.render_all_issues();
                this.adjust_height();
                this.show_or_hide_empty_warnings();
            }
        },

        wrap_assigned_issue: function(issue, done_percent, overdue) {
            return {
                internal_id: issue.internal_id,
                issue: issue,
                done_percent: done_percent || 0,
                overdue: overdue
            }
        },

        assign_issue: function(issue, employee, done_percent, overdue) {
            var issues = this.issues[employee] || [];
            this.issues[employee] = issues;

            var issue = this.wrap_assigned_issue(issue, done_percent, overdue);
            issues.push(issue);
            this.calculate_durations();

            var chart = this.get_assigned_issues_chart();
            var rows = chart.find('.cb-employee-row');
            var i;

            var index;
            for(i=0; i<rows.length; ++i) {
                if ($(rows[i]).attr('cb-employee') == employee) {
                    index = i;
                    break;
                }
            }

            var tr = rows[index];

            var issues_list = $(tr).find('.cb-assigned-issues-list');

            this.add_issue_to_assigned_list(issues_list, issue);
            this.render_issues(employee);
            this.actualize_timings(employee);
            this.show_or_hide_empty_warnings();
        },

        actualize_timings: function(employee_name) {
            var i;
            var table = this.get_assigned_issues_chart();
            var rows = $(table).find('.cb-employee-row');

            for(i=0; i<this.employees.length; ++i) {
                var employee = this.employees[i].name;
                if (employee != employee_name) {
                    continue;
                }

                var row = rows[i];

                var lines = $(row).find('.cb-assigned-issues-list').find('.cb-assigned-issues-item');
                var issues = this.issues[employee] || [];
                var j;
                for(j=0; j<issues.length; ++j) {
                    var line = lines[j];
                    var issue = issues[j];

                    if (this.is_finished(issue)) {
                        continue;
                    }

                    if (issue.starts_in == undefined) {
                        issue.starts_in = 40;
                    }

                    if (issue.ends_in == undefined) {
                        issue.ends_in = 40;
                    }

                    $(line).find('.cb-starts-in').html(this.format_timing(issue.starts_in));
                    $(line).find('.cb-ends-in').html(this.format_timing(issue.ends_in));
                }
            }
        },

        format_timing: function(offset) {
            if (offset == undefined) {
                return '';
            } else if (offset == 0) {
                return 'today';
            } else if (offset > 30) {
                return '> 1 month';
            } else if (offset < 70) {
                return "~ " + offset + " days";
            } else {
                var weeks = Math.round(offset / 7);
                return '~ ' + weeks + ' weeks';
            }
        },

        render: function() {
            this.calculate_durations();
            this.render_assigned_issues();
            this.render_timeline();
            this.render_velocities();
            this.render_dayoffs();
        },

        render_dayoffs: function() {
            var now = new Date();
            $("#cb-planning-date-from-for-day-off").val(to_iso_date(now));

            now.setDate(now.getDate() + 7);
            $("#cb-planning-date-to-for-day-off").val(to_iso_date(now));

            var i;
            var select = $("#cb-planning-employee-select-for-day-off");
            select.empty();
            select.append(`<option>All</option>`);

            for(i=0; i<this.employees.length; ++i) {
                var employee = this.employees[i].name;
                select.append(`<option>${employee}</option>`);
            }

            $("#cb-day-offs-chart").find(">tbody >tr").remove();
            for(i=0; i<this.day_offs.length; ++i) {
                var day_off = this.day_offs[i];

                CB.PLANNING.add_day_off(day_off.employee, day_off.date_from, day_off.date_to);
            }
        },

        commit_day_off_form: function() {
            var employee = $("#cb-planning-employee-select-for-day-off").val();
            var datetime_from = $("#cb-planning-date-from-for-day-off").val();
            var datetime_to = $("#cb-planning-date-to-for-day-off").val();

            CB._FORM_CLEAN = false;

            CB.PLANNING.add_day_off(employee, datetime_from, datetime_to);
            CB.PLANNING.actualize_day_offs();
        },

        ondelete_day_off: function(button) {
            CB._FORM_CLEAN = false;

            $(button).parents('tr').remove();
            this.actualize_day_offs();
        },

        actualize_day_offs: function() {
            var trs = $("#cb-day-offs-chart >tbody >tr");
            day_offs = [];
            for(i=0; i<trs.length; ++i) {
                var tr = trs[i];
                var tds = $(tr).find(">td");
                var employee = $(tds[0]).html();
                if (employee == 'All') {
                    employee = '';
                }
                day_offs.push({
                    employee: employee,
                    date_from: $(tds[1]).html(),
                    date_to: $(tds[2]).html()
                })
            }

            var i;
            for(i=0; i<this.days.length; ++i) {
                var day = this.days[i];

                day.employees_off = [];
                var j;
                for (j=0; j<day_offs.length; ++j) {
                    var day_off = day_offs[j];

                    if (day_off.date_from <= day.date && day.date<= day_off.date_to) {
                        day.employees_off.push(day_off.employee);
                    }
                }
            }

            this.render_all_issues();
        },

        add_day_off: function(employee, datetime_from, datetime_to) {
            if (!employee) {
                employee = 'All';
            } else {
                employee = employee;
            }

            $("#cb-day-offs-chart").prepend(
                "<tr>" +
                `<td>${employee}</td>` +
                `<td>${datetime_from}</td>` +
                `<td>${datetime_to}</td>` +
                `<td class='cb-text-align-center'><button class='red-168 white-255-text' onclick='CB.PLANNING.ondelete_day_off(this);'>X</button></td>` +
                "</tr>"
            );
        },

        update_history: function() {
            if (!this.internal_id) {
                return;
            }

            var self = this;
            $.ajax({
                url: URLS.planning.history + '/' + this.internal_id,
                dataType: 'json',
                crossDomain: true,
                method: 'get',
                data: {
                    datetime_from: format_date(this.datetime_from) + 'T00:00:00',
                    datetime_to: format_date(this.datetime_to) + 'T00:00:00' 
                },

                success: function(reply) {
                    console.log(reply);
                    self.history = reply;
                    self.render_history();
                },

                error: CB.process_http_error
            });

        },

        render_history: function() {
            var chart = this.get_chart();
            var tbody = $(chart).find('> tbody');
            var trs = $(tbody).find('> tr');

            var i;
            for(i=0; i<this.employees.length; ++i) {
                var employee = this.employees[i].name;

                var day__history = this.history[employee];
                if (!day__history) {
                    continue;
                }

                var tr = trs[i];
                var tds = $(tr).find("td");

                var j;
                for(j=0; j<day__history.length; ++j) {
                    var td = tds[j + 1];
                    var day = this.days[j];

                    if (day.is_today) {
                        break;
                    }

                    var issues = day__history[j];
                    if (!issues.length) {
                        continue;
                    }
                    var html = '';
                    var k;
                    for(k=0; k<issues.length; ++k) {
                        var issue = issues[k];
                        html += (
                            `<div class='cb-history-item'>`+
                            `<h1>${issue.key}</h1>` +
                            "</div>"
                        );
                    }
                    $(td).html(html);
                    $(td).addClass("cb-issue");
                }
            }

            for(i=0; i<this.employees.length; ++i) {
                var tr = trs[i];
                var tds = $(tr).find("td");

                var j;
                var prev_td = null;
                var colspan = 1;
                for(j=0; j<tds.length; ++j) {
                    var day = this.days[j];
                    if (day.is_today) {
                        break;
                    }
                    var td = tds[j + 1];
                    if ($(td).html() && prev_td && $(td).html() == $(prev_td).html()) {
                        colspan += 1;
                        $(prev_td).attr(
                            "colspan", colspan
                        );
                        $(td).remove();
                    } else {
                        prev_td = td;
                        colspan = 1;
                    }
                }
            }
        },

        render_velocities: function() {
            var table = this.get_velocities_chart();

            var body = $(table).find("tbody");
            $(body).empty();

            var i;
            for(i=0; i<this.employees.length; ++i) {
                var employee = this.employees[i];

                var tr = $(
                    "<tr>" +
                    `<td>${employee.name}</td>` +
                    `<td>${employee.velocity}</td>` +
                    "</tr>"
                );

                $(body).append($(tr));
            }
        },

        render_assigned_issues: function() {
            var i;
            var table = this.get_assigned_issues_chart();
            $(table).empty();
            for(i=0; i<this.employees.length; ++i) {
                var employee = this.employees[i].name;
                var velocity = this.employees[i].velocity;
                $tr = $(
                    "<div class='cb-employee-row'>" +
                    "<div class='cb-employee-info'>" +
                    `<div class="cb-employee">${employee}</div>` +
                    "</div>" +
                    '<select class="cb-jira-issue-select"></select>' +
                    '<table class="cb-assigned-issues-list">' +
                    '<thead style="display: none;"><tr><th></th><th>Key</th><th>Jira</th>' +
                    '<th>Status</th><th>Summary</th>' +
                    '<th>Size</th>' +
                    '<th>Finishes in</th>' +
                    '<th>Progress (%)</th>' +
                    '<th>Overdue</th>' +
                    '<th></th></tr></thead>' +
                    '<tbody></tbody>' +
                    '</table>' +
                    '<div class="cb-empty-warning">No issues yet...</div>' +
                    "</div>"
                );

                this.trs[employee] = i;
                $tr.attr('cb-employee', employee);
                $(table).append($tr);

                this.make_interactive_issue_select(
                    $tr.find(".cb-jira-issue-select")
                );

                var issues = this.issues[employee] || [];
                var j;
                for(j=0; j<issues.length; ++j) {
                    this.add_issue_to_assigned_list(
                        $tr.find(".cb-assigned-issues-list"),
                        issues[j]
                    );
                }
            }

        },

        render_all_issues: function() {
            var i;
            for(i=0; i<this.employees.length; ++i) {
                var employee = this.employees[i];

                this.render_issues(employee.name);
                this.actualize_timings(employee.name);
            }
        },

        make_interactive_issue_select: function($select) {
            var self = this;
            $select.select2({
                ajax: {
                    url: URLS.issues.options,
                    dataType: 'json',
                },

                placeholder: "Choose an issue to assign",
                allowClear: true
            });

            $select.on('select2:select', function(e) {
                var issue = e.params.data.issue;

                var $select = $(e.target);
                $select.val(null).trigger('change');

                var tr = $select.parents('.cb-employee-row');
                var employee_in_charge = $(tr).attr('cb-employee');

                var i;
                var already_assigned = false;
                for(i=0; i<self.employees.length; ++i) {
                    var employee = self.employees[i].name;
                    var assigned_issues = self.issues[employee] || [];
                    var j;
                    for(j=0; j<assigned_issues.length; ++j) {
                        var assigned_issue = assigned_issues[j];

                        if (assigned_issue.internal_id == issue.internal_id) {
                            CB.show_popup('Invalid action', `Issue is already assigned to ${employee}`);
                            already_assigned = true;
                        }
                    }
                }

                if (already_assigned) {
                    return;
                }

                $.ajax({
                    url: URLS.planning.get_done_percent,
                    dataType: 'json',
                    crossDomain: true,
                    data: {
                        issue_internal_id: issue.internal_id,
                        planning_internal_id: self.internal_id
                    },

                    error: CB.process_http_error,
                    success: function(result) {
                        self.assign_issue(issue, employee_in_charge, result.done_percent, result.overdue);
                    }
                });

            });
        },

        render_issues: function(employee) {
            var index = this.trs[employee];

            var $tr = $(this.get_chart().find('> tbody').find('> tr')[index]);

            var i = 0;
            var j = 1;

            var tds = $tr.find('> td');

            var td = tds[0];
            var issues_list = $(td).find('.cb-assigned-issues-list');

            for(j=1; j<tds.length; ++j) {
                var td = tds[j];
                $(td).remove();
            }

            var issues = this.issues[employee] || [];
            j = 0;
            for(; j<this.days.length; ++j) {
                if (this.days[j].is_today) {
                    break;
                }

                $tr.append(`<td class='cb-past'></td>`);
            }
            var offset = 0;

            for (i=0; i<issues.length; ++i) {
                var issue = issues[i];

                if (this.is_finished(issue)) {
                    continue;
                }
                issue.starts_in = undefined;
                issue.ends_in = undefined;

                if (offset) {
                    issue.starts_in = offset + 1;
                } else {
                    issue.starts_in = 0;
                }
                var target_duration = Math.max(issue.duration, 1);
                var real_duration = 0;
                var prev_real_duration = 0;
                var duration = 0;
                for(; j<this.days.length; ++j) {
                    var day = this.days[j];

                    if (target_duration <= duration) {
                        $tr.append(
                            `<td class='cb-issue' colspan="${real_duration}">` +
                            `<span>${issue.issue.key} ${issue.issue.summary}</span></td>`
                        );
                        issue.ends_in = offset;
                        break;
                    }

                    offset += 1;

                    if (day.weekday > 4 || contains(day.employees_off, '') || contains(day.employees_off, employee)) {
                        is_off = true;
                    } else {
                        is_off = false;
                    }

                    if (is_off) {
                        if (real_duration) {
                            $tr.append(
                                `<td class='cb-issue' colspan="${real_duration}">` +
                                `<span>${issue.issue.key} ${issue.issue.summary}</span></td>`
                            );
                            $tr.append(`<td class='cb-day-off'></td>`);
                            real_duration = 0;
                        } else {
                            $tr.append(`<td class='cb-day-off'></td>`);
                        }

                    } else {
                        duration += 1;
                        real_duration += 1;
                    }

                    if (j == (this.days.length - 1) && real_duration) {
                        $tr.append(
                            `<td class='cb-issue' colspan="${real_duration}">` +
                            `${issue.issue.key} ${issue.issue.summary}</td>`
                        );
                    }
                }
            }
            for(; j<this.days.length; ++j) {
                var day = this.days[j];
                var klass = '';
                var text = '';
                if (day.weekday > 4 || contains(day.employees_off, '') || contains(day.employees_off, employee)) {
                    klass = 'cb-day-off';
                    text = '';
                }

                $tr.append(`<td class='${klass}'>${text}</td>`);
            }
        },

        render_timeline: function() {
            $.ajax({
                url: URLS.planning.calendar,
                dataType: 'json',
                crossDomain: true,
                method: 'get',
                data: {
                    planning_internal_id: this.internal_id,
                    days_qty: this.days_qty,
                    base_dt: format_date(this.datetime_from) 
                },

                success: function(reply) {
                    CB.PLANNING.days = reply.days;
                    CB.PLANNING.months = reply.months;
                    CB.PLANNING._render_timeline();
                    CB.PLANNING.render_all_issues();
                    CB.PLANNING.adjust_height();
                    CB.PLANNING.show_or_hide_empty_warnings();
                    CB.PLANNING.update_history();
                },

                error: CB.process_http_error
            });
        },

        adjust_height: function() {
            return;
            var table = this.get_chart();

            var trs = $(table).find('>tbody').find(">tr");
            var i;
            for(i=0; i<trs.length; ++i) {
                var tr = trs[i];
                var issues_list = $(tr).find('.cb-assigned-issues-list');
                var height = $(issues_list).height();
                if (height == 0) {
                    height = 50;
                }
                $(tr).find('> td').height(height + 130);
            }
        },

        _render_timeline: function() {
            var chart = this.get_chart();
            var thead = $(chart).find('> thead');

            var header_trs = $(thead).find('tr');

            var tr1 = header_trs[0];
            var tr2 = header_trs[1];

            var ths1 = $(tr1).find('th');
            var i;
            for(i=1; i<ths1.length; ++i) {
                $(ths1[i]).remove();
            }

            for(i=0; i<this.months.length; ++i) {
                var month = this.months[i];
                $(tr1).append(`<th colspan="${month.length}">${month.name}</th>`);
            }

            var ths2 = $(tr2).find('th');
            for(i=1; i<ths2.length; ++i) {
                $(ths2[i]).remove();
            }

            var is_past = true;
            for(i=0; i<this.days.length; ++i) {
                var day = this.days[i];

                klass = `cb-week-day-${day.weekday + 1}`;
                if (day.is_today) {
                    klass += ' cb-today';
                    is_past = false;
                }

                if (is_past) {
                    klass += ' cb-past';
                }
                if (day.weekday > 4) {  // 0 = Monday
                    klass += ' cb-holiday';
                }

                $(tr2).append(`<th class="${klass}">${day.id}</th>`);
            }

            var tbody = $(chart).find('> tbody');
            tbody.empty();

            var i;
            for(i=0; i<this.employees.length; ++i) {
                tbody.append("<tr></tr>");
            }

            var trs = $(tbody).find('> tr');

            for(i=0; i<trs.length; ++i) {
                var tr = trs[i];
                var employee = this.employees[i].name;
                var $td = $(`<td class='cb-employee'>${employee}</td>`);
                $(tr).append($td);

                var is_past = true;
                for(j=0; j<this.days.length; ++j) {
                    var day = this.days[j];
                    var klass = `test`;

                    if (day.is_today) {
                        klass += ' cb-today';
                        is_past = false;
                    }

                    if (is_past) {
                        klass += ' cb-past';
                    }

                    var text = '';
                    var $td = $(`<td class='${klass}'>${text}</td>`);
                    $(tr).append($td);
                }
            }
        }
    };

    CB.render_plannings_table = function() {
        var clean = CB.confirm_form_close(function() {
            CB.render_plannings_table();
        });

        if (!clean) {
            return;
        }

        location.hash = "plannings";
        CB.set_active_menu("#cb-plannings-menu");

        CB.hide_all_innerblocks();
        var $panel = $("#cb-plannings-table");
        var $tbody = $panel.find('tbody')

        $.ajax({
            url: URLS.planning.list,
            dataType: 'json',
            crossDomain: true,

            success: function(plannings) {
                $tbody.empty();

                var i;
                for(i=0; i<plannings.length; i++) {
                    var d = plannings[i];

                    $tbody.append(
                        `<tr onclick="CB.render_planning_form('${d.internal_id}');">` +
                        `<td>${d.name}</td>` +
                        "</tr>"
                    );
                }
                $panel.show(100);
            },
            error: CB.process_http_error
        });
    }

    CB.render_planning_form = function(internal_id, defaults) {
        var clean = CB.confirm_form_close(function() {
            CB.render_planning_form(internal_id, defaults);
        });

        if (!clean) {
            return;
        }

        location.hash = `planning:${internal_id || ''}`;
        CB.set_active_menu("#cb-plannings-menu");

        var $form = $("#cb-planning-form");
        var $legend = $("#cb-planning-form > legend");
        $("#cb-planning-form-errors").hide();

        var _finish_form = function() {
            CB.hide_all_innerblocks();

            var base_dt = new Date();
            var days_qty = 30;

            CB.PLANNING.days_qty = days_qty;
            CB.PLANNING.datetime_from = base_dt;
            CB.PLANNING.datetime_to = (new Date(base_dt)).setDate(base_dt.getDate() + days_qty);
            CB.PLANNING.render();

            CB._FORM_CLEAN = true;
            $form.show(100);
        }

        $form.find(".cb-metric-input").select2({
            ajax: {
                url: URLS.metrics.options,
                dataType: 'json',
            },

            allowClear: true
        });

        if (internal_id) {
            $.ajax({
                url: URLS.planning.list + "/" + internal_id,

                dataType: 'json',
                crossDomain: true,

                success: function(reply) {
                    var planning = reply.planning
                    var employees = reply.employees;

                    $legend.text("Planning [" + planning.name + "]");
                    $form.find(".cb-internal-id-input").val(planning.internal_id);
                    $form.find(".cb-name-input").val(planning.name);

                    var newOption = new Option(
                        planning.velocity_metric.name, planning.velocity_metric.internal_id,
                        true, true
                    );
                    $form.find(".cb-metric-input").append(newOption).trigger('change');

                    $form.find(".cb-default-issue-size-input").val(planning.default_issue_size);
                    $form.find(".cb-period-input").val(planning.velocity_period);
                    $form.find(".cb-issue-size-field-input").val(planning.issue_size_field);
                    $form.find(".cb-terminal-states-input").val(planning.terminal_states.join("\n"));

                    CB.PLANNING.internal_id = planning.internal_id;
                    CB.PLANNING.employees = employees;
                    CB.PLANNING.issues = planning.assigned_issues;
                    CB.PLANNING.issue_size_field = planning.issue_size_field;
                    CB.PLANNING.default_issue_size = planning.default_issue_size;
                    CB.PLANNING.terminal_states = planning.terminal_states;
                    CB.PLANNING.day_offs = planning.day_offs;

                    _finish_form();
                },
                error: CB.process_http_error
            });

        } else {
            $legend.html("Creating a new planning");
            $("#cb-planning-charts").show();

            $form.find(".cb-internal-id-input").val('');
            $form.find(".cb-name-input").val('Default name');

            $form.find(".cb-metric-input").empty().trigger('change');

            $form.find(".cb-default-issue-size-input").val('1');
            $form.find(".cb-period-input").val('30d');
            $form.find(".cb-issue-size-field-input").val('custom_fields.storypoints');
            $form.find(".cb-terminal-states-input").val('Closed');

            CB.PLANNING.clear();

            _finish_form();
        }
    }

    CB.render_metric_form = function(internal_id, defaults) {
        var clean = CB.confirm_form_close(function() {
            CB.render_metric_form(internal_id, defaults);
        });

        if (!clean) {
            return;
        }

        location.hash = `metric:${internal_id || ''}`;
        CB.set_active_menu("#cb-metrics-menu");

        var $form = $("#cb-metric-form");
        var $legend = $("#cb-metric-form > legend");

        CB.empty_metric_form();

        var _finish_form = function() {
            CB.hide_all_innerblocks();
            var $form = $("#cb-metric-form");
            $(".tui-tab.active").trigger('click')
            CB.actualize_stage_inputs();
            $form.show(100);
        }
        if (internal_id) {
            $.ajax({
                url: URLS.metrics.list + "/" + internal_id,

                dataType: 'json',
                crossDomain: true,

                success: function(metric) {
                    $legend.text("Query [" + metric.name + "]");
                    $form.find(".cb-internal-id-input").val(metric.internal_id);
                    $form.find(".cb-name-input").val(metric.name);
                    var i;

                    for(i=0; i<metric.pipeline.length; ++i) {
                        var stage = metric.pipeline[i];
                        CB.add_metric_pipeline_stage(
                            stage,
                        );

                    } 

                    for(i=0; i<metric.variables.length; ++i) {
                        var variable = metric.variables[i];
                        CB.add_metric_variable_spec(variable.name, variable.type);
                    }
                    CB.configure_metric_debug_variables();
                    _finish_form();
                },
                error: CB.process_http_error
            });

        } else {
            $legend.html("Creating a new metric");
            if (defaults) {
                $form.find(".cb-name-input").val(defaults.name);
                var i;

                for(i=0; i<defaults.pipeline.length; ++i) {
                    var stage = defaults.pipeline[i];
                    CB.add_metric_pipeline_stage(
                        stage,
                    );

                } 
            } else {
                if (CB._METRICS_ROOT) {
                    $form.find(".cb-name-input").val(CB._METRICS_ROOT);
                }
                CB.add_metric_pipeline_stage();
            }
            _finish_form();
        }
    }

    CB._METRICS_ROOT = '';
    CB._DASHBOARDS_ROOT = '';

    CB.render_metrics_table = function(root, escaped) {
        var clean = CB.confirm_form_close(function() {
            CB.render_metrics_table(root, escaped);
        });

        if (!clean) {
            return;
        }

        location.hash = "metrics";
        CB.set_active_menu("#cb-metrics-menu");

        if (root && escaped) {
            root = unescape(root);
        }

        CB._METRICS_ROOT = root;
        CB.hide_all_innerblocks();
        var $panel = $("#cb-metrics-table");
        var $tbody = $panel.find('tbody')

        $.ajax({
            url: URLS.metrics.list,
            dataType: 'json',
            crossDomain: true,
            data: {
                root: root
            },

            success: function(metrics) {
                $tbody.empty();

                var super_root = metrics.super_root;
                if (super_root !== null) {
                    super_root = escape(super_root);
                    var tr = $(
                        `<tr class='cb-catalog' onclick="CB.render_metrics_table('${super_root}', true);">` +
                        "<td>&#9776;&nbsp;..</td>" +
                        "</tr>"
                    ); 
                    $tbody.append(tr);
                }

                var i;
                var catalogs = (metrics.grouped || []).sort();
                for(i=0; i<catalogs.length; ++i) {
                    c = catalogs[i];
                    c1 = escape(c);  //.replaceAll('"', '\\"').replaceAll("'", "\\'");
                    $tbody.append(
                        `<tr class='cb-catalog' onclick='CB.render_metrics_table("${c1}", true);'>` +
                        "<td>&#9776;&nbsp;" + c + "</td>" +
                        "</tr>"
                    );
                }

                var offset = '';
                if (catalogs.length) {
                    offset = '&nbsp;'
                }

                var orphans = (metrics.orphans || []).sort();
                for(i=0; i<orphans.length; i++) {
                    var d = orphans[i];

                    $tbody.append(
                        "<tr onclick=\"CB.render_metric_form('" + d.internal_id + "');\">" +
                        "<td>" + offset + ' ' + d.name + "</td>" +
                        "</tr>"
                    );

                }
                $panel.show(100);
            },
            error: CB.process_http_error
        });
    }

    CB.get_chart_options = function(internal_id) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]')

        var chart_type = $elem.parents('.cb-metric-presentation-block').find(".cb-chart-type").val();
        var stacked = $elem.parents('.cb-metric-presentation-block').find(".cb-stack-data:checked").length > 0;

        var type = 'splineArea';
        var opacity = 1;
        if (stacked) {
            if (chart_type == 'area') {
                type = 'stackedArea';
                opacity = 0.8;
            } else {
                type = 'stackedColumn';
            }
        } else {
            if (chart_type == 'area') {
                type = 'splineArea';
                opacity = 0.5;
            } else {
                type = 'column';
            }
        }

        return {
            type: type,
            opacity: opacity
        }

    }

    CB.hide_chart_series = function(internal_id) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]')
        var chart_id = $elem.attr('chart_id');
 
         var chart = CB.CHARTS[chart_id];

        var i;
        for(i=0; i<chart.data.length; ++i) {
            chart.data[i].set("visible", false);    
        }
    }

    CB.show_chart_series = function(internal_id) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]')
        var chart_id = $elem.attr('chart_id');
 
        var chart = CB.CHARTS[chart_id];

        var i;
        for(i=0; i<chart.data.length; ++i) {
            chart.data[i].set("visible", true);    
        }
    }

    CB.apply_options_to_chart = function(internal_id) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]')
        var chart_id = $elem.attr('chart_id');
 
         var chart = CB.CHARTS[chart_id];
        var options = CB.get_chart_options(internal_id);

        var i;
        for(i=0; i<chart.data.length; ++i) {
            chart.data[i].set("type", options.type);    
            chart.data[i].set("fillOpacity", options.opacity);    
        }
    }

    CB.add_new_metric_to_dashboard = function() {
        CB._FORM_CLEAN = false;

        var popup = $("#cb-add-new-metric-popup");
        popup.find(".tui-panel-header").html("Choose query");

        var $table = popup.find('.cb-metrics-table');
        CB.add_controls_to_table($table);

        var $tbody = $table.find('tbody');

        $.ajax({
            url: URLS.metrics.list,
            dataType: 'json',
            crossDomain: true,
            data: {
                mode: 'flat'
            },

            success: function(metrics) {
                $tbody.empty();

                var i;
                for(i=0; i<metrics.length; i++) {
                    var d = metrics[i];

                    $tbody.append(
                        "<tr onclick=\"CB.add_metric_to_dashboard('" + d.internal_id + "', true); CB.close_popup(this);\">" +
                        "<td>" + d.name + "</td>" +
                        "</tr>"
                    );

                }
            },
            error: CB.process_http_error
        });

        // popup.find(".cb-popup-text").html(html);

        popup.show();
    }    

    CB.add_metric_to_dashboard = function(internal_id, prepend, metric_type) {
        var $slots = $(".cb-metric-presentation");

        for(i=0; i<$slots.length; ++i) {
            var $slot = $($slots[i]);

            var existing_internal_id = $slot.attr('internal_id');
            if (existing_internal_id == internal_id) {
                CB.show_popup('OK', 'Metric is already added.');
                return;
        }
        }

        metric_type = metric_type || 'table';

        var $container = $("#cb-dashboard-form .cb-metrics-presentation-container");
        var $form = $(
            "<fieldset draggable='true' class='" + "cb-metric-presentation-block" + "'>" +
            "<legend>" + '' + "</legend>" +
            "<div class='cb-controls-container'>" +
            "<div class='cb-label'>Metric type:</div> <select " +
            "onchange='CB.apply_metric_type_to_chart(\"" + internal_id + "\")' " +
            "class=\"tui-input2 cb-metric-type\">" +    
            "<option>table</option>" +
            "<option>timeseries</option>" +
            "</select>" +
            "<a href='https://github.com/panaetov/captain-bridge/wiki/Dashboards#switching-between-chart-and-table' class='cb-question-mark' target='_blank'>&quest;</a>" +
            "<br>" +

            "<div class='cb-controls'>" +
            "</div>" +
            "</div>" +
            "<div class='cb-metric-presentation ui-widget-content' " +
            "internal_id='" + internal_id + "'>" +
            "</div>" +
            "</fieldset>"
        );
        if (prepend) {
            $container.prepend($form);
        } else {
            $container.append($form);
        }

        $form.find(".cb-metric-type").val(metric_type);
        CB.render_metric_controls(internal_id);

        $.ajax({
            url: URLS.metrics.list + "/" + internal_id,

            dataType: 'json',
            crossDomain: true,

            success: function(metric) {
                var $el = $('.cb-metric-presentation[internal_id="' + internal_id + '"]');
                var fieldset = $el.parents('.cb-metric-presentation-block');
                var legend = fieldset.find('legend');

                var legend_html = (
                    `<span class='cb-metric-legend-name'>` +
                    `<a href='/#metric:${metric.internal_id}' target='_blank'>` +
                    `Metric: "<span class='cb-the-name'>${metric.name}</span> ` +
                    `<img src='/front/images/external-link.png'>"</a>` +

                    "<button class='cb-microbutton' onclick='CB.move_metric_up(this);'>&#8593;</button>" + 
                    "<button class='cb-microbutton' onclick='CB.move_metric_down(this);'>&#8595;</button>" +

                    "<button class='tui-button cb-float-right red-168 white-text cb-remove-metric-button' " +
                    `onclick='CB.remove_metric_from_dashboard(this);' ` +
                    ">X</button>" +

                    `</span>` 
                );
                // `<button class='cb-edit-metric-button tui-button'><a href='/#metric:${metric.internal_id}' target='_blank'>Edit</a></button>`
                legend.html(legend_html);


                var i;
                for(i=0; i<metric.variables.length; ++i) {
                    var variable = metric.variables[i];

                    CB.add_dashboard_variable(variable);
                }
                CB.refresh_metric(internal_id);
            },
            error: CB.process_http_error
        });
    }

    CB.apply_metric_type_to_chart = function(internal_id) {
        var $slot = $(".cb-metrics-presentation-container");
        var $elem = $slot.find('[internal_id="' + internal_id + '"]')

        CB.render_metric_controls(internal_id) ;
        CB.refresh_metric(internal_id);
    }

    CB.remove_metric_from_dashboard = function(button) {
        CB._FORM_CLEAN = false;

        var $button = $(button);
        var $block = $button.parents('.cb-metric-presentation-block');
        $block.remove();
    }

    CB.move_metric_up = function(button) {
        CB._FORM_CLEAN = false;

        var $button = $(button);
        
        var $block = $button.parents('.cb-metric-presentation-block');
        var $before = $block.prev();
        $block.insertBefore($before);
    }

    CB.move_metric_down = function(button) {
        CB._FORM_CLEAN = false;

        var $button = $(button);

        var $block = $button.parents('.cb-metric-presentation-block');
        var $after = $block.next();
        $block.insertAfter($after);
    }

    CB.add_form_error = function(errors_container, error_text) {
        var error = "<span class='cb-bold'>ERROR: </span><span>" + error_text + "</span><br/>"; 
        $(errors_container).append(error);
    }

    CB.validate_dashboard_form = function() {
        var $form = $("#cb-dashboard-form");

        var $errors = $("#cb-dashboard-form-errors");
        $errors.empty();

        var name = $form.find('.cb-name-input').val();
        if (!name.trim()) {
            CB.add_form_error($errors, "Name is empty.");
            return false;
        }
        return true;
    }

    CB.parse_variables_options = function() {
        var containers = $("#cb-variables .cb-variable");
        var i;

        var result = [];

        // datetime_from, datetime_to, step must be passed
        for(i=3; i<containers.length; ++i) {
            var container = $(containers[i]);

            var name = container.find('.cb-variable-name').val();
            var options = container.find('.cb-variable-options').val();
            result.push({
                name: name,
                options: options
            });
        }
        return result;
    }

    CB.save_dashboard = function() {
        var is_valid = CB.validate_dashboard_form();
        if (!is_valid) {
            $("#cb-dashboard-form-errors").show();
            return;
        } else {
            $("#cb-dashboard-form-errors").hide();
        }

        $form = $("#cb-dashboard-form");
        var variables = CB.parse_variables_options();

        var $metrics = $form.find('.cb-metric-presentation');

        var i;
        var metric_ids = [];
        var metric_types = {};

        for(i=0;i<$metrics.length; ++i) {
            var $metric = $($metrics[i]);

            var metric_id = $metric.attr('internal_id');
            metric_ids.push(metric_id);

            var metric_type = $metric.parents(".cb-metric-presentation-block").find('.cb-metric-type').val();
            metric_types[metric_id] = metric_type;
        }

        $.ajax({
            url: URLS.dashboards.save,
            method: 'post',
            dataType: 'json',
            crossDomain: true,
            contentType: "application/json",

            data: JSON.stringify({
                internal_id: $form.find(".cb-internal-id-input").val(),
                name: $form.find(".cb-name-input").val(),
                metrics: metric_ids,
                metric_types: metric_types,
                variables: variables
            }),

            success: function(result) {
                CB.show_popup('OK', 'Dashboard is saved');
                CB._FORM_CLEAN = true;
                $form.find(".cb-internal-id-input").val(result.internal_id);
                location.hash = `dashboard:${result.internal_id || ''}`;
                CB.set_active_menu("#cb-dashboards-menu");
            },
            error: CB.process_http_error
        });

    }

    CB.refresh_dashboard = function() {
        var $slots = $(".cb-metric-presentation");

        for(i=0; i<$slots.length; ++i) {
            var $slot = $($slots[i]);

            var internal_id = $slot.attr('internal_id');
            CB.refresh_metric(internal_id);
        }
    }

    CB.render_metric_controls = function(internal_id) {
        var $el = $('.cb-metric-presentation[internal_id="' + internal_id + '"]');
        var fieldset = $el.parents('.cb-metric-presentation-block');

        var metric_type = $(fieldset).find('.cb-metric-type').val();
        var $controls = fieldset.find('.cb-controls');

        $controls.empty();

        if (metric_type == 'timeseries') {

            $controls.append($(
                "<label class=\"tui-checkbox2\">Stack data:</label><br>" +
                "<input type=\"checkbox\" class='cb-stack-data'" +
                "onchange='CB.apply_options_to_chart(\"" + internal_id + "\")'/>" +
                "<span></span>" +
                "" +
                "<div class='cb-label'>Graphic type:</div><select " +
                "onchange='CB.apply_options_to_chart(\"" + internal_id + "\")' " +
                "class=\"tui-input2 cb-chart-type\">" +    
                "<option>area</option>" +
                "<option>bar</option>" +
                "</select><br>" +
                "<button class='tui-button cb-chart-toggler' " + 
                "onclick='CB.hide_chart_series(\"" + internal_id + "\")'" +
                ">Hide all series</button>" +
                "<button class='tui-button cb-chart-toggler' " +
                "onclick='CB.show_chart_series(\"" + internal_id + "\")'" +
                ">Show all series</button>" +
                "<br/>"
            ));
        }
    }

    CB.parse_variables = function(containers) {
        var containers = containers || $("#cb-variables .cb-variable");

        var result = {};
        var i;
        for (i=0; i<containers.length; ++i) {
            var container = $(containers[i]);
            var inputs = container.find("input,select");
            var name = $(inputs[0]);
            var value = $(inputs[1]);

            if (name.val()) {
                result[name.val()] = value.val() || "";
            }
        }
        return result;
    }

    CB.parse_interval = str => {
        let seconds = 0;
        let days = str.match(/(\d+)\s*d$/);
        let hours = str.match(/(\d+)\s*h$/);
        let minutes = str.match(/(\d+)\s*m$/);
        if (days) { seconds += parseInt(days[1])*86400; }
        if (hours) { seconds += parseInt(hours[1])*3600; }
        if (minutes) { seconds += parseInt(minutes[1])*60; }
        return seconds;
    };

    CB.refresh_metric = function(internal_id) {
        var $datetime_from = $("#cb-dashboard-form .cb-datetime-from");
        var $datetime_to = $("#cb-dashboard-form .cb-datetime-to");
        var $period = $("#cb-dashboard-form .cb-period");

        if (!$datetime_from.val()) {
            CB.show_popup(
                "Invalid parameters",
                "Parameter <span class='cb-bold'>datetime_from</span> is not correct. Please check it."
            );
            return;
        }

        if (!$datetime_to.val()) {
            CB.show_popup(
                "Invalid parameters",
                "Parameter <span class='cb-bold'>datetime_to</span> is not correct. Please check it."
            );
            return;
        }

        if (!CB.parse_interval($period.val())) {
            CB.show_popup(
                "Invalid parameters",
                "Value of <span class='cb-bold'>step</span> is not a valid interval. Please check it."
            );
            return;
        }


        var $slot = $(".cb-metrics-presentation-container");
        var data_type = $slot.find(
            '[internal_id="' + internal_id + '"]'
        ).parents(
            '.cb-metric-presentation-block'
        ).find(".cb-metric-type").val();

        var variables = CB.parse_variables();
        $.ajax({
            url: URLS.metrics.run,
            method: 'post',
            dataType: 'json',
            crossDomain: true,
            contentType: "application/json",

            data: JSON.stringify({
                internal_id: internal_id,
                variables: variables,
                datetime_from: variables.datetime_from,
                datetime_to: variables.datetime_to,
                period: variables.step,
                metric_type: data_type
            }),

            success: function(reply) {
                if (reply.error) {
                    CB.render_metric_error(internal_id, reply.error);
                    return;
                }

                var data = reply.data;

                if (data_type == 'timeseries') {
                    CB.render_metric_timeseries_chart(internal_id, data);
                } else if (data_type == 'table') {
                    CB.render_metric_table(internal_id, data);
                }
            },
            error: function(jq, status, error) {
                CB.render_metric_error(internal_id, `Server unavailable, details = "${error}".`);
            }
        });

    }

    CB.DASHBOARD = {};

    CB.render_dashboard_form = function(internal_id) {
        var clean = CB.confirm_form_close(function() {
            CB.render_dashboard_form(internal_id);
        });

        if (!clean) {
            return;
        }

        location.hash = `dashboard:${internal_id || ''}`;
        CB.set_active_menu("#cb-dashboards-menu");

        CB.hide_all_innerblocks();
        var $panel = $("#cb-dashboards-table");

        var $form = $("#cb-dashboard-form");
        var $legend = $("#cb-dashboard-form > legend");

        CB.empty_dashboard_form();

        var _finish_form = function() {
            CB.hide_all_innerblocks();
            $form.show(100);
        }
        if (internal_id) {
            $.ajax({
                url: URLS.dashboards.list + "/" + internal_id,

                dataType: 'json',
                crossDomain: true,

                success: function(dashboard) {
                    CB.DASHBOARD = dashboard;
                    $legend.text("Dashboard <" + dashboard.name + ">");
                    $form.find(".cb-internal-id-input").val(dashboard.internal_id);
                    $form.find(".cb-name-input").val(dashboard.name);
                    $form.find(".cb-period").val('14d');

                    var i;
                    var $container = $("#cb-dashboard-form .cb-metrics-presentation-container");
                    $container.empty();
                    for(i=0; i<dashboard.metrics.length; ++i) {
                        var metric_internal_id = dashboard.metrics[i];

                        CB.add_metric_to_dashboard(
                            metric_internal_id,
                            false,
                            dashboard.metric_types[metric_internal_id],
                        );
                    }
                    // CB.refresh_dashboard();
                    for(i=0; i<dashboard.variables.length; ++i) {
                        // CB.add_dashboard_variable(
                        //     dashboard.variables[i]
                        // );
                    }

                    _finish_form();
                },
                error: CB.process_http_error
            });

        } else {
            $legend.html("Creating a new dashboard");
            $form.find(".cb-name-input").val(CB._DASHBOARDS_ROOT);
            _finish_form();
        }

    }

    CB.render_dashboards_table = function(root) {
        var clean = CB.confirm_form_close(function() {
            CB.render_dashboards_table(root);
        });

        if (!clean) {
            return;
        }

        location.hash = "dashboards";
        CB.set_active_menu("#cb-dashboards-menu");

        CB._DASHBOARDS_ROOT = root;
        CB.hide_all_innerblocks();
        var $panel = $("#cb-dashboards-table");
        var $tbody = $panel.find('tbody')

        $.ajax({
            url: URLS.dashboards.list,
            dataType: 'json',
            crossDomain: true,
            data: {
                root: root
            },

            success: function(dashboards) {
                $tbody.empty();

                var super_root = dashboards.super_root;
                if (super_root !== null) {
                    $tbody.append(
                        "<tr class='cb-catalog' onclick=\"CB.render_dashboards_table('" + super_root + "');\">" +
                        "<td>&#9776;&nbsp;..</td>" +
                        "</tr>"
                    );
                }

                var i;
                var catalogs = (dashboards.grouped || []).sort();
                for(i=0; i<catalogs.length; ++i) {
                    c = catalogs[i];
                    $tbody.append(
                        "<tr class='cb-catalog' onclick=\"CB.render_dashboards_table('" + c + "');\">" +
                        "<td>&#9776;&nbsp;" + c + "</td>" +
                        "</tr>"
                    );
                }

                var offset = '';
                if (catalogs.length) {
                    offset = '&nbsp;'
                }
                var orphans = (dashboards.orphans || []).sort();
                for(i=0; i < orphans.length; i++) {
                    var d = orphans[i];

                    $tbody.append(
                        "<tr onclick=\"CB.render_dashboard_form('" + d.internal_id + "');\">" +
                        "<td>" + offset + ' ' + d.name + "</td>" +
                        "</tr>"
                    );

                }
                $panel.show(100);
            },
            error: CB.process_http_error
        });
    }

    CB.render_readme = function() {
        var clean = CB.confirm_form_close(function() {
            CB.render_readme();
        });

        if (!clean) {
            return;
        }

        CB.hide_all_innerblocks();
        $("#cb-mainblock").hide();
        $("#cb-readme").show(100);
    }

    CB.set_active_menu = function(menu_id) {
        $(".cb-header-item").removeClass("active");
        $(menu_id).addClass("active");
    }


    CB.generate_timeseries_format_error = function() {
        return "README of format";
    }

    var HASH = location.hash;

    if (HASH == '#dashboards') {
        CB.render_dashboards_table();

    } else if (HASH == '#metrics') {
        CB.render_metrics_table();

    } else if (HASH == '#jiras') {
        CB.render_jiras_table();

    } else if (HASH == '#redmines') {
        CB.render_redmines_table();

    } else if (HASH.startsWith("#jira:")) {
        var internal_id = HASH.split(":")[1];
        CB.render_jira_form(internal_id);

    } else if (HASH.startsWith("#redmine:")) {
        var internal_id = HASH.split(":")[1];
        CB.render_redmine_form(internal_id);

    } else if (HASH.startsWith("#metric:")) {
        var internal_id = HASH.split(":")[1];
        CB.render_metric_form(internal_id);

    } else if (HASH.startsWith("#dashboard:")) {
        var internal_id = HASH.split(":")[1];
        CB.render_dashboard_form(internal_id);

    } else if (HASH.startsWith("#planning:")) {
        var internal_id = HASH.split(":")[1];
        CB.render_planning_form(internal_id);
    } else if (HASH.startsWith("#plannings")) {
        CB.render_plannings_table();
    }




})
