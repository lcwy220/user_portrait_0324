function user_rank_timepicker(str){
    var date_time = str.split(' ');
    var dates = date_time[0].split('/');
    var yy = parseInt(dates[0]);
    var mm = parseInt(dates[1]) - 1;
    var dd = parseInt(dates[2]);
    var times = date_time[1].split(':');
    var hh = parseInt(times[0]);
    var minute = parseInt(times[1]);
    var final_date = new Date();
    final_date.setFullYear(yy,mm,dd);
    final_date.setHours(hh,minute);
    final_date = Math.floor(final_date.getTime()/1000);
    return final_date;
}

function detect_task_status (data) {
	$('#detect_task_status').empty();
	var html = '';
	html += '<table class="table table-striped" style="margin-left:30px;width:900px;">';
	for(var i=0;i<data.length;i++){
		html += '<tr>';
		html += '<td style="width:200px;text-align:center;">'+data[i][0]+'</td>';
		html += '<td>'+data[i][1]+'</td>';
		html += '</tr>';
	}
	html += '</table>';
	$('#detect_task_status').append(html);
}

//入库用户列表
function draw_user_in_table(data){
    //var data = [];
    $('#mood_in_user').empty();
    if(data.length == 0){
        $('#mood_in_user').append('<h4 style="text-align:center;">暂无数据</h4>');
    }else{
        if(data.length > 5){
            show_more_inuser(data);
            data = data.slice(0,5);
        }else{
            $('#showmore_inuser').css('display', 'none');
        }
        var html = '';
        html += '<table  class="table table-striped table-bordered bootstrap-datatable datatable responsive">';
        html += '<thead><th style="text-align:center;">用户ID</th>';
        html += '<th style="text-align:center;">昵称</th>';
        html += '<th style="text-align:center;">影响力</th>';
        html += '<th style="text-align:center;">身份敏感度</th>';
        html += '<th style="text-align:center;">活跃度</th>';
        html += '<th style="text-align:center;">敏感度</th></thead>';
        for(var i=0;i<data.length;i++){
            html += '<tr>';
            html += '<td style="text-align:center;">'+data[i][0]+'</td>';
            html += '<td style="text-align:center;">'+data[i][1]+'</td>';
            html += '<td style="text-align:center;">'+data[i][2]+'</td>';
            html += '<td style="text-align:center;">'+data[i][3]+'</td>';
            html += '<td style="text-align:center;">'+data[i][4]+'</td>';
            html += '<td style="text-align:center;">'+data[i][5]+'</td>';
            html += '</tr>';
        }
        html += '</table>';
        $('#mood_in_user').append(html);

    }
}

//模态框入库用户
function show_more_inuser(data){
    $('#inuser_WordList').empty();
    var html = '';
    html += '<table id="more_inuser_table" class="table table-striped table-bordered bootstrap-datatable datatable responsive">';
    html += '<thead><th style="text-align:center;">用户ID</th>';
    html += '<th style="text-align:center;">昵称</th>';
    html += '<th style="text-align:center;">影响力</th>';
    html += '<th style="text-align:center;">身份敏感度</th>';
    html += '<th style="text-align:center;">活跃度</th>';
    html += '<th style="text-align:center;">敏感度</th></thead>';
    for(var i=0;i<data.length;i++){
        html += '<tr>';
        html += '<td style="text-align:center;">'+data[i][0]+'</td>';
        html += '<td style="text-align:center;">'+data[i][1]+'</td>';
        html += '<td style="text-align:center;">'+data[i][2]+'</td>';
        html += '<td style="text-align:center;">'+data[i][3]+'</td>';
        html += '<td style="text-align:center;">'+data[i][4]+'</td>';
        html += '<td style="text-align:center;">'+data[i][5]+'</td>';
        html += '</tr>';
    }
    html += '</table>';
    $('#inuser_WordList').append(html);
    $('#more_inuser_table').dataTable({
    "sDom": "<'row'<'col-md-6'l ><'col-md-6'f>r>t<'row'<'col-md-12'i><'col-md-12 center-block'p>>",
    "sPaginationType": "bootstrap",
    //"aoColumnDefs":[ {"bSortable": false, "aTargets":[1]}],
    "oLanguage": {
        "sLengthMenu": "每页 _MENU_ 条 ",
    }
    });
}

//出库用户列表
function draw_user_out_table(data){
    //console.log(data);
    $('#out_user_title').css('display', 'block');
    $('#mood_out_user').empty();
    if(data.length == 0){
        $('#mood_out_user').append('<h4 style="text-align:center;">暂无数据</h4>');
    }else{
        if(data.length > 5){
            show_more_outuser(data);
            data = data.slice(0,5);
        }else{
            $('#showmore_outuser').css('display', 'none');
        }
        var html = '';
        html += '<table class="table table-striped table-bordered bootstrap-datatable datatable responsive">';
        html += '<thead><th style="text-align:center;">用户ID</th>';
        html += '<th style="text-align:center;">昵称</th>';
        html += '<th style="text-align:center;">微博数</th>';
        html += '<th style="text-align:center;">粉丝数</th>';
        html += '<th style="text-align:center;">关注数</th></thead>';
        for(var i=0;i<data.length;i++){
            html += '<tr>';
            html += '<td style="text-align:center;">'+data[i][0]+'</td>';
            html += '<td style="text-align:center;">'+data[i][1]+'</td>';
            html += '<td style="text-align:center;">'+data[i][2]+'</td>';
            html += '<td style="text-align:center;">'+data[i][3]+'</td>';
            html += '<td style="text-align:center;">'+data[i][4]+'</td>';
            html += '</tr>';
        }
        html += '</table>';
        $('#mood_out_user').append(html);
    }
}

//模态框出库用户
function show_more_outuser(data){
    $('#outuser_WordList').empty();
    var html = '';
    html += '<table id="more_outuser_table" class="table table-striped table-bordered bootstrap-datatable datatable responsive">';
    html += '<thead><th style="text-align:center;">用户ID</th>';
    html += '<th style="text-align:center;">昵称</th>';
    html += '<th style="text-align:center;">微博数</th>';
    html += '<th style="text-align:center;">粉丝数</th>';
    html += '<th style="text-align:center;">关注数</th></thead>';
    for(var i=0;i<data.length;i++){
        html += '<tr>';
        html += '<td style="text-align:center;">'+data[i][0]+'</td>';
        html += '<td style="text-align:center;">'+data[i][1]+'</td>';
        html += '<td style="text-align:center;">'+data[i][2]+'</td>';
        html += '<td style="text-align:center;">'+data[i][3]+'</td>';
        html += '<td style="text-align:center;">'+data[i][4]+'</td>';
        html += '</tr>';
    }
    html += '</table>';
    $('#outuser_WordList').append(html);
    $('#more_outuser_table').dataTable({
    "sDom": "<'row'<'col-md-6'l ><'col-md-6'f>r>t<'row'<'col-md-12'i><'col-md-12 center-block'p>>",
    "sPaginationType": "bootstrap",
    //"aoColumnDefs":[ {"bSortable": false, "aTargets":[1]}],
    "oLanguage": {
        "sLengthMenu": "每页 _MENU_ 条 ",
        }
    });
}

//展示微博
function Draw_get_top_weibo(data, div_name){
  var html = '';
  $('#' + div_name).empty();
  console.log(div_name);
    if(data[0][3] == ''){
        html += "<div style='margin-left:10px;width:100%;height:100px;'>用户在昨天未发布任何微博</div>";
    }else{
      html += '<div id="weibo_list" class="weibo_list weibo_list_height scrolls tang-scrollpanel" style="margin:0;">';
      html += '<div id="content_control_height" class="tang-scrollpanel-wrapper" style="margin:0;">';
      html += '<div class="tang-scrollpanel-content" style="margin:0;">';
      html += '<ul>';
      for(var i=0;i<data.length;i++){
        s = (i+1).toString();
        var weibo = data[i]
        var mid = weibo[0];
        var uid = weibo[9];
        var name = weibo[10];
        var date = weibo[5];
        var text = weibo[3];
        var geo = weibo[4];
        var reposts_count = weibo[1];
        var comments_count = weibo[2];
        var weibo_link = weibo[7];
        var user_link = weibo[8];
        var profile_image_url = 'http://tp2.sinaimg.cn/1878376757/50/0/1';
        var repost_tree_link = '/show_graph/' + mid;
        if (geo==''){
           geo = '未知';
        }
        var user_link = 'http://weibo.com/u/' + uid;
        html += '<li class="item">';
        html += '<div class="weibo_detail" style="width:100%">';
        html += '<p style="text-align:left;margin-bottom:0;">' +s + '、昵称:<a class="undlin" target="_blank" href="' + user_link  + '">' + name + '</a>(' + geo + ')&nbsp;&nbsp;发布内容：&nbsp;&nbsp;' + text + '</p>';
        html += '<div class="weibo_info"style="width:100%">';
        html += '<div class="weibo_pz">';
        html += '<div id="topweibo_mid" class="hidden">'+mid+'</div>';
        html += '<span class="retweet_count">转发数(' + reposts_count + ')</span>&nbsp;&nbsp;|&nbsp;&nbsp;';
        html += '<span class="comment_count">评论数(' + comments_count + ')</span></div>';
        html += '<div class="m">';
        html += '<u>' + date + '</u>&nbsp;-&nbsp;';
        html += '<a target="_blank" href="' + weibo_link + '">微博</a>&nbsp;-&nbsp;';
        html += '<a target="_blank" href="' + user_link + '">用户</a>';
        html += '<a target="_blank" href="' + repost_tree_link + '">&nbsp;-&nbsp;转发树</a>';
        html += '</div>';
        html += '</div>';
        html += '</div>';
        html += '</li>';
      }
                                    
    html += '<div id="TANGRAM_54__slider" class="tang-ui tang-slider tang-slider-vtl" style="height: 100%;">';
    html += '<div id="TANGRAM_56__view" class="tang-view" style="width: 6px;">';
    html += '<div class="tang-content"><div id="TANGRAM_56__inner" class="tang-inner"><div id="TANGRAM_56__process" class="tang-process tang-process-undefined" style="height: 0px;"></div></div></div>';
    html += '<a id="TANGRAM_56__knob" href="javascript:;" class="tang-knob" style="top: 0%; left: 0px;"></a></div>';
    html += '<div class="tang-corner tang-start" id="TANGRAM_54__arrowTop"></div><div class="tang-corner tang-last" id="TANGRAM_54__arrowBottom"></div></div>';

    html += '</ul>';
    html += '</div>';
    html += '</div>';
    html += '</div>';   
    }
      $('#'+div_name).append(html);
}

//显示所有的相关微博
function show_all_related_weibo(url) {
    $('#weibo_sort').empty();
    //$('#related_weibo').empty();
    var html = '';
    //html += '<div style="border-bottom: 3px solid #dddddd;height: 40px;line-height:40px;">';
    html += '<span style="color:#983333;margin-left: 20px;"><b>排序指标</b></span>';
    html += '<input type="radio" name="sort_radio_weibo" id="sort_by_time" value = "time" checked="checked" style="margin-left: 30px;"> 时间';
    html += '<input type="radio" name="sort_radio_weibo" value = "retweet" style="margin-left: 30px;"> 转发数';
    html += '<input type="radio" name="sort_radio_weibo" value = "comment" style="margin-left: 30px;"> 评论数';
    //html += '<div id="related_weibo_text0" style="width:100%;max-height: 300px;">'; 
    //html += '</div>';
    $('#weibo_sort').append(html);
    
    var sub_html = '<div id="related_weibo_text0" class="shadow_weibo" style="width:100%;"></div>'; 
    $('#sub_related_weibo').append(sub_html);

    var data2 = [['12','10','12','根本实现不了两会代表委员们应该提案:汽车分公母[笑cry]，男的开母车，母车限速；女的开公车，公车不要油门。','中国 北京 北京','2013-09-07 00:10:90','fgeeeesf','sfagvfd','sfagvfd','1234567890','昵称昵称'],
                 ['12','10','12','根本实现不了两会代表委员们应该提案:汽车分公母[笑cry]，男的开母车，母车限速；女的开公车，公车不要油门。','中国 北京 北京','2013-09-07 00:10:90','fgeeeesf','sfagvfd','sfagvfd','1234567890','昵称昵称'],
                 ['12','10','12','根本实现不了两会代表委员们应该提案:汽车分公母[笑cry]，男的开母车，母车限速；女的开公车，公车不要油门。','中国 北京 北京','2013-09-07 00:10:90','fgeeeesf','sfagvfd','sfagvfd','1234567890','昵称昵称']]

    var sort_type = $('input[name="sort_radio_weibo"]:checked').val();
    console.log(sort_type);
    Draw_get_top_weibo(data2, "related_weibo_text0");

    $('input[name="sort_radio_weibo"]').off('click').click(function(){
        var sort_type = $('input[name="sort_radio_weibo"]:checked').val();
        console.log(sort_type);
        if(sort_type == "time"){
            alert('mmm1')
            //Draw_get_top_weibo(data2, panel_name);
        };
        if(sort_type == "retweet"){
            console.log('retweet1');
        }
        if(sort_type == "comment"){
            console.log('comment1');
        }
    });
}


//选择相应的微博,获取点击的按钮的id，控制对应面板显示。
function choose_related_weibo(url, index){
    $('.portrait_button_choose').die('click').live("click", function (){
    var data2 = [['12','10','12','根本实现不了两会代表委员们应该提案:汽车分公母[笑cry]，男的开母车，母车限速；女的开公车，公车不要油门。','中国 北京 北京','2013-09-07 00:10:90','fgeeeesf','sfagvfd','sfagvfd','1234567890','昵称昵称'],
                 ['12','10','12','根本实现不了两会代表委员们应该提案:汽车分公母[笑cry]，男的开母车，母车限速；女的开公车，公车不要油门。','中国 北京 北京','2013-09-07 00:10:90','fgeeeesf','sfagvfd','sfagvfd','1234567890','昵称昵称'],
                 ['12','10','12','根本实现不了两会代表委员们应该提案:汽车分公母[笑cry]，男的开母车，母车限速；女的开公车，公车不要油门。','中国 北京 北京','2013-09-07 00:10:90','fgeeeesf','sfagvfd','sfagvfd','1234567890','昵称昵称']]

        $('#sort_by_time').attr("checked",true);
        click_id = $(this).attr('id');
        click_id = click_id.split('choose');
        var panel_name = 'related_weibo_text' + click_id[1];
        console.log(panel_name);
        $('.shadow_weibo').css('display', 'none');
        $('#'+panel_name).css('display', 'block');

        $('input[name="sort_radio_weibo"]').off('click').click(function(){
            var sort_type = $('input[name="sort_radio_weibo"]:checked').val();
            console.log(sort_type);
            if(sort_type == "time"){
                alert('mmm')
                //Draw_get_top_weibo(data2, panel_name);
            };
            if(sort_type == "retweet"){
                console.log('retweet');
            }
            if(sort_type == "comment"){
                console.log('comment');
            }
        });

        //console.log(sort_type);

    })
}


//相关主题
function show_related_topic(data){
    $('#topic_key').empty();
    $('input[name="sub_topic"]').attr("checked",false);
    $('#sub_related_weibo_button').empty();
    $('#sub_related_weibo').empty();

    //话题表格
    var html = '';
    html += '<table id="more_topic_table" class="table table-striped">';
    html += '<tr><th style="text-align:center;width:200px;border-right:1px solid #CCCCCC">全部</th>';
    html += '<th style="text-align:center;">';
    for(var i=0;i<data.length;i++){
        html += '<span style="margin-right:20px;">'+data[i][0]+'</span>';
    };
    html += '</th></tr>';
    html += '</table>';
    $('#topic_key').append(html);

    //进一步计算子话题
    var call_flag = 0;
    var topic_count = 0;
    $("input[name='sub_topic']").off('click').click(function(){
        if($("input[name='sub_topic']:checked")){
            //console.log(call_flag);
            if(call_flag == 0){
                //call_ajax();
                //有子话题的话请求数据，flag保证请求一次，同时在表格上加上子话题，加上按钮。写出各个按钮的url
                var data2 = [['省道','而非','该时段'], ['算法'], ['上午','前往']];
                topic_count = data2.length;
                var html = '';
                for(var j=0; j<data2.length; j++){
                    html = '<tr><th style="text-align:center;width:200px;border-right:1px solid #CCCCCC">话题'+(j+1)+'</th>';
                    html += '<th style="text-align:center;">' ;
                    for(var s=0; s<data2[j].length;s++){
                        html += '<span style="margin-right:20px;">'+data2[j][s]+'</span>';
                    };
                    html += '</th></tr>';
                    $('#more_topic_table').append(html);


                };
            }
 
        }
        if(call_flag == 0){
            for(var j=0;j<topic_count;j++){
                var button_html = '<span id="portrait_button_choose'+(j+1)+'" class="portrait_button_choose" style="height:30px;cursor:pointer;margin-right:20px;text-align: center;line-height:30px;">话题' +(j+1)+ '</span>';
                $('#sub_related_weibo_button').append(button_html);

                var tab_html = '<div id="related_weibo_text'+(j+1)+'" class="shadow_weibo" style="display:none;width:900px;">'+(j+1)+'</div>';
                $('#sub_related_weibo').append(tab_html);
            }
            call_flag += 1;  
        }

    });


    //显示所有微博,请求数据
    var call_all_url;

    show_all_related_weibo(call_all_url);

    //点击事件选择微博，传有几个子话题
    var call_url;
    choose_related_weibo(call_url, topic_count);

}

function show_detail(flag, time, sentiment){
    //var data = [['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56']]
    var data = [['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56'],['1234567890','这是昵称','23.33','32.43','24.674','33.56']]
    $('#result_detect_detail').css('display','block');
    $('#click_time').empty();
    $('#click_sentiment').empty();
    $('#click_time').append(time);
    $('#click_sentiment').append(sentiment);
    console.log(flag);
    if(flag == 0){
        draw_user_in_table(data);
        $('#mood_out_all').empty();
        $('#mood_in_all').css('width', '900px');
    }else{
        $('#mood_in_all').css('width', '440px');
        draw_user_in_table(data);
        draw_user_out_table(data);

    }
    //相关话题表格及微博详情
    show_related_topic(data);

}


function Draw_detect_charts(flag){
    var data_x_ = [];
    var data_y_ = [];

    for(var i=0;i<data.length;i++){
        var time_line  = new Date(parseInt(data[i][0])*1000).format("yyyy-MM-dd hh: mm");
        data_x_.push(time_line);
        data_y_.push(data[i][1]);

    }

    $('#result_detect_charts').highcharts({
        chart: {
            type: 'spline',// line,
            animation: Highcharts.svg, // don't animate in old IE
            style: {
                fontSize: '13px',
                fontFamily: 'Microsoft YaHei'
            }},
        title: {
            text: '微博情绪走势图',
            x: -20, //-20：center
            style: {
                color: '#555555',
                fontSize: '14px'
            }
        },
        
        lang: {
            printChart: "打印",
            downloadJPEG: "下载JPEG 图片",
            downloadPDF: "下载PDF文档",
            downloadPNG: "下载PNG 图片",
            downloadSVG: "下载SVG 矢量图",
            exportButtonTitle: "导出图片"
        },
        xAxis: {
            //categories: data_x,
            categories: ['周一','周二','周三','周四','周午','周六','周日'],
            labels:{
              rotation: 0,
              //step: 15,
              y:25
            }
        },
        yAxis: {
            min: 0,
            title: {
                text: '微博总量 (条)'
            },
            plotLines: [{
                value: 0,
                width: 1,
                color: '#808080'
            }]
        },
        plotOptions:{
            series:{            
                cursor:'pointer',
                events:{
                    click:function(event){
                        //用flag标识是库内还是全网
                        console.log('哪一条线','时间x','值y')
                        console.log(this.name,event.point.category, event.point.y);
                        //
                        show_detail(flag, event.point.category, this.name);
                    }
                }
            }
        },
        tooltip: {
            valueSuffix: '条',
            xDateFormat: '%Y-%m-%d %H:%M:%S'
        },
        legend: {
            //enabled: false,
            layout: 'vertical',
            align: 'right',
            verticalAlign: 'top',
            borderWidth: 0
        },
        series: [
            {
                name:'积极',
                data: [19,23,45,23,25,32,17]
            },
            {
                name:'消极',
                data: [21,33,25,23,15,22,37]
            },            
            {
                name:'中性',
                data: [10,37,15,13,35,12,47]
            },
        ]
    });
}

//排序范围选择
$('#detect_choose').change(function(){
    $('#detect_choose_detail').empty();
    //库内-不限
    if($('#detect_choose').val() == 'in_nolimit') {

        $('#detect_time_choose').empty();
        var time_html = '';     
        time_html += '<span><input name="time_range" type="radio" value="1" checked="checked"> 过去一天</span>';
        time_html += '<span><input name="time_range" type="radio" value="7" style="margin-left:20px;"> 过去七天</span>';
        time_html += '<span><input name="time_range" type="radio" value="30" style="margin-left:20px;"> 过去一个月</span>';
        $('#detect_time_choose').append(time_html);
    }
    //库内-领域
    if($('#detect_choose').val() == 'in_limit_domain') {

        $('#detect_time_choose').empty();
        var time_html = '';     
        time_html += '<input name="time_range" type="radio" value="1" checked="checked"> 过去一天';
        time_html += '<input name="time_range" type="radio" value="7" style="margin-left:20px;"> 过去七天';
        time_html += '<input name="time_range" type="radio" value="30" style="margin-left:20px;"> 过去一个月';
        $('#detect_time_choose').append(time_html);

        var html = '';
        html += '<select id="detect_choose_detail_2">';
        html += '<option value="">境内机构</option>';
        html += '<option value="">境外机构</option>';
        html += '<option value="">民间组织</option>';
        html += '<option value="">境外媒体</option>';
        html += '<option value="">活跃人士</option>';
        html += '<option value="">商业人士</option>';
        html += '<option value="">媒体人士</option>';
        html += '<option value="">高校</option>';
        html += '<option value="">草根</option>';
        html += '<option value="">媒体</option>';
        html += '<option value="">法律机构及人士</option>';
        html += '<option value="">政府机构及人士</option>';
        html += '<option value="">其他</option>';
        html += '</select>';
    };
    //库内-话题
    if($('#detect_choose').val() == 'in_limit_topic') {

        $('#detect_time_choose').empty();
        var time_html = '';     
        time_html += '<input name="time_range" type="radio" value="1" checked="checked"> 过去一天';
        time_html += '<input name="time_range" type="radio" value="7" style="margin-left:20px;"> 过去七天';
        time_html += '<input name="time_range" type="radio" value="30" style="margin-left:20px;"> 过去一个月';
        $('#detect_time_choose').append(time_html);

        var html = '';
        html += '<select id="detect_choose_detail_2">';
        html += '<option value="">科技类</option>';
        html += '<option value="">经济类</option>';
        html += '<option value="">教育类</option>';
        html += '<option value="">军事类</option>';
        html += '<option value="">民生类_健康</option>';
        html += '<option value="">民生类_住房</option>';
        html += '<option value="">民生类_环保</option>';
        html += '<option value="">民生类_就业</option>';
        html += '<option value="">民生类_社会保障</option>';
        html += '<option value="">民生类_交通</option>';
        html += '<option value="">民生类_法律</option>';
        html += '<option value="">政治类_外交</option>';
        html += '<option value="">政治类_暴恐</option>';
        html += '<option value="">政治类_地区和平</option>';
        html += '<option value="">政治类_反腐</option>';
        html += '<option value="">政治类_宗教</option>';
        html += '<option value="">文体类_娱乐</option>';
        html += '<option value="">文体类_体育</option>';
        html += '<option value="">其他类</option>';
        html += '</select>';
    };
    
    //全网-all
    if($('#detect_choose').val() == 'all_nolimit') {

        $('#detect_time_choose').empty();
        var time_html = '';     
        time_html += '<input name="time_range" type="radio" value="1" checked="checked"> 过去一天';
        time_html += '<input name="time_range" type="radio" value="7" style="margin-left:20px;"> 过去七天';
        time_html += '<input name="time_range" type="radio" value="30" style="margin-left:20px;"> 过去一个月';
        $('#detect_time_choose').append(time_html);

    }
    //全网-关键词
    if($('#detect_choose').val() == 'all_limit_keyword') {

        $('#detect_time_choose').empty();
        var time_html = '';
        time_html += '<input id="weibo_from" type="text" class="form-control" style="width:145px; display:inline-block;height:25px;">&nbsp;-&nbsp;';
        time_html += '<input id="weibo_to" type="text" class="form-control" style="width:145px; display:inline-block;height:25px">';        
        $('#detect_time_choose').append(time_html);
        date_init();

        var html = '';
        html += '<input id="keyword_hashtag" type="text" class="form-control" style="width:145px;height:25px;" placeholder="请输入关键词">';

    };
    $('#detect_choose_detail').append(html);
});

//筛选条件初始化时间
function date_init(){
    var date = choose_time_for_mode();
    date.setHours(0,0,0,0);
    var max_date = date.format('yyyy/MM/dd hh:mm');
    var current_date = date.format('yyyy/MM/dd hh:mm');
    var from_date_time = Math.floor(date.getTime()/1000) - 60*60*24;
    var min_date_ms = new Date()
    min_date_ms.setTime(from_date_time*1000);
    var from_date = min_date_ms.format('yyyy/MM/dd hh:mm');
    if(global_test_mode==0){
        $('#detect_time_choose #weibo_from').datetimepicker({value:from_date,step:60});
        $('#detect_time_choose #weibo_to').datetimepicker({value:current_date,step:60});
    }else{
        $('#detect_time_choose #weibo_from').datetimepicker({value:from_date,step:60,minDate:'-1970/01/30',maxDate:'+1970/01/01'});
        $('#detect_time_choose #weibo_to').datetimepicker({value:current_date,step:60,minDate:'-1970/01/30',maxDate:'+1970/01/01'});
    }
}

//提交监控
function submit_detect(){
    var s = [];
    var show_scope = $('#detect_choose option:selected').text();
    var show_arg = $('#detect_choose_detail_2 option:selected').text();
    var show_norm = $('#sort_select_2 option:selected').text();
    var keyword = $('#keyword_hashtag').val();
    var sort_scope = $('#detect_choose option:selected').val();
    var sort_norm = $('#sort_select_2 option:selected').val();
    var arg = $('#detect_choose_detail_2 option:selected').text();
    var day_select = $("input[name='time_range']:checked").val();
    //console.log(keyword);
    if(keyword == ''){  //检查输入词是否为空
        alert('请输入关键词！');
    }else{
        if(keyword == undefined){  //没有输入的时候，更新图表
            var flag = 0; //0是库内，1代表全网
            var url = 'time='+day_select+'&sort_norm='+sort_norm+'&sort_scope='+sort_scope;
            var data = [['111关键词：两会','111状态：正在计算'],['关键词：两会','状态：正在计算'],['关键词：两会','状态：正在计算']];
            $('#detect_range').empty();
            $('#detect_detail').empty();
            $('#detect_rank_by').empty();
            $('#detect_time_range').empty();
            $('#detect_range').append(show_scope);
            console.log(arg);
            if(sort_scope != 'in_nolimit' && sort_scope != 'all_nolimit' ){  // 参数是可选的时候，加上详细条件
                $('#detect_detail').append('-');
                $('#detect_detail').append(show_arg);
                url += '&arg='+arg;   //该参数为空时不传
            }
            $('#detect_rank_by').append(show_norm);
            if(day_select == "1"){
                $('#detect_time_range').append('过去一天');
            }
            if(day_select == "7"){
                $('#detect_time_range').append('过去七天');
            }
            if(day_select == "30"){
                $('#detect_time_range').append('过去一个月');
            }
            if(sort_scope == 'all_nolimit'){
                flag = 1;
            }
            Draw_detect_charts(flag);
            $('#result_detect_detail').css('display','none');
            console.log(url);
        }else{ //输入参数的时候，更新任务状态表格
            var time_from = user_rank_timepicker($('#detect_time_choose #weibo_from').val());
            console.log(time_from)
            var time_to = user_rank_timepicker($('#detect_time_choose #weibo_to').val());
            var time_from_after = new Date(time_from*1000)
            var time_to_after = new Date(time_to*1000)
            time_from_after = time_from_after.format('yyyy-MM-dd')
            time_to_after = time_to_after.format('yyyy-MM-dd')
            console.log(time_from_after)
	    //var time_from = user_rank_timepicker($('#detect_time_choose #weibo_from').val());
            //var time_to = user_rank_timepicker($('#detect_time_choose #weibo_to').val());
            var url = 'time='+time_from_after+','+time_to_after+'&sort_norm='+sort_norm+'&sort_scope='+sort_scope+'&arg='+keyword;
            var data = [['121关键词：两会','121状态：正在计算'],['关键词：两会','状态：正在计算'],['关键词：两会','状态：正在计算']];
            detect_task_status(data);
            console.log(url);
        }
    }
}

//结果分析默认值
$('#detect_range').append($('#detect_choose option:selected').text());
// $('#detect_detail').append('：');
// $('#detect_detail').append($('#detect_choose option:selected').text());
$('#detect_rank_by').append($('#sort_select_2 option:selected').text());
var day_select = $("input[name='time_range']:checked").val();
if(day_select == "1"){
    $('#detect_time_range').append('过去一天')
}
if(day_select == "7"){
    $('#detect_time_range').append('过去七天')
}
if(day_select == "30"){
    $('#detect_time_range').append('过去三十天')
}

var data = [['关键词：两会','状态：正在计算'],['关键词：两会','状态：正在计算'],['关键词：两会','状态：正在计算']];
detect_task_status(data);
Draw_detect_charts(1);
