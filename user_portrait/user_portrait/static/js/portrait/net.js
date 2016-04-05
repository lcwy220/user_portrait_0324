function hidden_keywords(){
    $('#show_keywords').addClass('hidden');
	$('#by_keywords').css('background-color','#6699FF');
	$('#framecontent').removeClass('hidden');
	$('#by_time').css('background-color','#3351B7');
}
function hidden_time(){
    $('#framecontent').addClass('hidden');
	$('#by_time').css('background-color','#6699FF');
    $('#show_keywords').removeClass('hidden');
	$('#by_keywords').css('background-color','#3351B7');
}
$(function(){
    temporal_rank_table('asdsad');
});
function temporal_rank_table(data){
	$('#result_rank_table').empty();
	var html = '';
	html += '<table id="rank_table" class="table table-striped table-bordered bootstrap-datatable datatable responsive" style="margin-left:30px;width:900px;">';
	html += '<thead><th style="text-align:center;">排名</th>';
	html += '<th style="text-align:center;">用户ID</th>';
	html += '<th style="text-align:center;">昵称</th>';
	html += '<th style="text-align:center;">是否入库</th>';
	html += '<th style="text-align:center;">注册地</th>';
	html += '<th style="text-align:center;">粉丝数</th>';
	html += '<th style="text-align:center;">微博数</th>';
	html += '<th style="text-align:center;">评论量</th>';
	html += '<th style="text-align:center;">转发量</th></thead>';
	
	for(var i=0;i<10;i++){
	    /*
		var uid = data[i][0];
		var uname = data[i][1];
		if(uname == ''){
			uname = '未知';
		}
		var sign_loca = data[i][3];
		if(sign_loca == ''){
			sign_loca = '未知'
		}
		if(data[i][7]==1){ //是否入库
			var ifin = '是';
		}else{
			var ifin = '否';
		}
		if(data[i][4]==''){//fans
			data[i][4]= '未知';
		}
		if(data[i][5]==''){//retweeted
			data[i][5]= '未知';
		}
		if(data[i][6]==''){//comment
			data[i][6]= '未知';
		}
		if(data[i][2]==''){//weibo
			data[i][2]= '未知';
		}
		
		html += '<tr>';
		html += '<td style="text-align:center;">'+(i+1)+'</td>';
		html += '<td style="text-align:center;"><a href="/index/personal/?uid='+uid+'" target="_blank">'+uid+'</a></td>';
		html += '<td style="text-align:center;">'+uname+'</td>';
		html += '<td style="text-align:center;">'+ifin+'</td>';
		html += '<td style="text-align:center;">'+sign_loca+'</td>';
		html += '<td style="text-align:center;">'+data[i][4]+'</td>';
		html += '<td style="text-align:center;">'+data[i][2]+'</td>';
		html += '<td style="text-align:center;">'+data[i][5]+'</td>';
		html += '<td style="text-align:center;">'+data[i][6]+'</td>';
		html += '</tr>';
		*/
		html += '<tr>';
		html += '<td style="text-align:center;">test</td>';
		html += '<td style="text-align:center;">test</td>';
		html += '<td style="text-align:center;">test</td>';
		html += '<td style="text-align:center;">test</td>';
		html += '<td style="text-align:center;">test</td>';
		html += '<td style="text-align:center;">test</td>';
		html += '<td style="text-align:center;">test</td>';
		html += '<td style="text-align:center;">test</td>';
		html += '<td style="text-align:center;">test</td>';
		html += '</tr>';
	}
	html += '</table>';
	$('#result_rank_table').append(html);
};



$(function () {
    $('#Activezh').highcharts({
        title: {
            text: 'Monthly Average Temperature',
            x: -20 //center
        },
        subtitle: {
            text: 'Source: WorldClimate.com',
            x: -20
        },
        xAxis: {
            categories: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun','Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        },
        yAxis: {
            title: {
                text: 'Temperature (°C)'
            },
            plotLines: [{
                value: 0,
                width: 1,
                color: '#808080'
            }]
        },
        tooltip: {
            valueSuffix: '°C'
        },
        legend: {
            layout: 'vertical',
            align: 'right',
            verticalAlign: 'middle',
            borderWidth: 0
        },
        series: [{
            name: 'Tokyo',
            data: [7.0, 6.9, 9.5, 14.5, 18.2, 21.5, 25.2, 26.5, 23.3, 18.3, 13.9, 9.6]
        }, {
            name: 'New York',
            data: [-0.2, 0.8, 5.7, 11.3, 17.0, 22.0, 24.8, 24.1, 20.1, 14.1, 8.6, 2.5]
        }, {
            name: 'Berlin',
            data: [-0.9, 0.6, 3.5, 8.4, 13.5, 17.0, 18.6, 17.9, 14.3, 9.0, 3.9, 1.0]
        }, {
            name: 'London',
            data: [3.9, 4.2, 5.7, 8.5, 11.9, 15.2, 17.0, 16.6, 14.2, 10.3, 6.6, 4.8]
        }]
    });
});


//提交监控
function submit_detect(){
    var s = [];
    var show_scope = $('#detect_choose option:selected').text();
    var show_arg = $('#detect_choose_detail_2 option:selected').text();
    var show_norm = $('#sort_select_2 option:selected').text();
    var keyword = $('#keyword_detect').val();
    var sort_scope = $('#detect_choose option:selected').val();
    var sort_norm = $('#sort_select_2 option:selected').val();
    var arg = $('#detect_choose_detail_2 option:selected').val();
    scope_arg = arg; 

    var time_from =$('#detect_time_choose #weibo_from').val().split('/').join('-');
    var time_to =$('#detect_time_choose #weibo_to').val().split('/').join('-');
    var from_stamp = new Date($('#detect_time_choose #weibo_from').val());
    var end_stamp = new Date($('#detect_time_choose #weibo_to').val());
    if(from_stamp > end_stamp){
        alert('起始时间不得大于终止时间！');
        return false;
    }
    //console.log(keyword);
    if(keyword == ''){  //检查输入词是否为空
        alert('请输入关键词！');
    }else{
        if(keyword == undefined){  //没有输入的时候，更新图表
            var url = 'start_date='+time_from+'&end_date='+time_to+'&segment='+sort_norm;
            if(sort_scope == 'all_nolimit'){
                //flag = 1;
                var all_url ='';
                all_url += '/sentiment/sentiment_all/?' +url;
                console.log(all_url);
                call_sync_ajax_request(all_url, Draw_detect_all_charts);
            };
            if(sort_scope == 'in_nolimit'){
                var in_url = '';
                in_url += '/sentiment/sentiment_all_portrait/?' +url;
                console.log(in_url);
                call_sync_ajax_request(in_url, Draw_in_all_detect_charts);
            }
            if(sort_scope == 'in_limit_topic'){
                var topic_url = '/sentiment/sentiment_topic/?' + url +'&topic='+arg;
                console.log(topic_url);
                call_sync_ajax_request(topic_url, Draw_in_topic_detect_charts);
            }
            if(sort_scope == 'in_limit_domain'){
                var domain_url = '/sentiment/sentiment_domain/?' + url +'&domain='+arg;
                console.log(domain_url);
                call_sync_ajax_request(domain_url, Draw_in_domain_detect_charts);
            }
            //var data = {"flag": true, "data": [{"sort_norm": "bci", "status": 1, "keyword": "hello2", "sort_scope": "in_limit_hashtag", "start_time": "2013-09-03", "submit_user": "admin@qq.com", "search_type": "hashtag", "end_time": "2013-09-04", "search_id": "admin@qq.com1459093215.85"}, {"sort_norm": "bci_change", "status": 1, "keyword": "\u4e2d\u56fd\u4eba\u6c11", "sort_scope": "all_limit_keyword", "start_time": "2013-09-03", "submit_user": "admin@qq.com", "search_type": "keyword", "end_time": "2013-09-06", "search_id": "admin@qq.com1459093370.92"}, {"sort_norm": "imp", "status": 1, "keyword": "456", "sort_scope": "in_limit_hashtag", "start_time": "2013-09-03", "submit_user": "admin@qq.com", "search_type": "hashtag", "end_time": "2013-09-04", "search_id": "admin@qq.com1459095146.65"}, {"sort_norm": "bci", "status": 1, "keyword": "hello", "sort_scope": "in_limit_hashtag", "start_time": "2013-09-02", "submit_user": "admin@qq.com", "search_type": "hashtag", "end_time": "2013-09-03", "search_id": "admin@qq.com1459091263.5"}]};
            $('#detect_range').empty();
            $('#detect_detail').empty();
            $('#detect_rank_by').empty();
            $('#detect_time_range').empty();
            $('#detect_range').append(show_scope);

            if(sort_scope == 'in_limit_topic' || sort_scope == 'in_limit_domain' ){  // 参数是可选的时候，加上详细条件
                $('#detect_range').append('-');
                $('#detect_range').append(show_arg);
            }
            $('#detect_rank_by').append(show_norm);
            var time_from_end = time_from + ' 至 ' + time_to;
            $('#detect_time_range').append(time_from_end);                       
            $('#result_detect_detail').css('display','none');

        }else{ //输入参数的时候，更新任务状态表格
            var keyword_array = [];
            var keyword_array = keyword.split(',');
            var keyword_string = keyword_array.join(',');
            var url = '/sentiment/submit_sentiment_all_keywords/?start_date='+time_from+'&end_date='+time_to+'&keywords='+keyword_string +'&submit_user=' + username +'&segment='+sort_norm;
            call_sync_ajax_request(url, submit_detect_offline)
            //detect_task_status(data);
            console.log(url);
        }
    }
}

//离线任务删除
function de_del(data){
    console.log(data);
    if(data == true){
        alert('删除成功！');
        var task_url = '/sentiment/search_sentiment_all_keywords_task/?submit_user='+username;
        call_sync_ajax_request(task_url, detect_task_status);
    }else{
        alert('删除失败，请再试一次！');
    }
}
//搜索任务提交
function search_task(){
    var submit_date = $('#weibo_modal').val().split('/').join('-');
    var start_date = $('#weibo_from_modal').val().split('/').join('-');
    var end_date = $('#weibo_to_modal').val().split('/').join('-');
    var submit_key = $('#search_key').val();
    var search_url = '/sentiment/search_sentiment_all_keywords_task/?submit_user='+username;
    if(submit_key != ''){
        search_url += '&keywords='+submit_key;
    }
    //var status = $('input[name="search_status"]:checked').val();
    var status = $('#search_status').val();
    console.log(status);
    if(status != "2"){
        search_url += '&status=' +status;
    };

    var status= $('')
    if($('#time_checkbox').is(':checked')){
       search_url += '&start_date='+start_date+'&end_date='+end_date;
    };
    if($(' #time_checkbox_submit').is(':checked')){
        search_url += '&submit_date='+submit_date;
    }
    console.log(search_url);

    call_sync_ajax_request(search_url, detect_task_status);
}
