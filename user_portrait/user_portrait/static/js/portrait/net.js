function call_sync_ajax_request(url, callback){
    $.ajax({
      url: url,
      type: 'GET',
      dataType: 'json',
      async: true,
      success:callback
    });
}
var myDate = new Date();
var month = myDate.getMonth()+1;
var day = myDate.getDate();
var show_day = month.toString() +'月'+ day.toString() +'日';
var hh = myDate.getHours();
var mm =myDate.getMinutes();
var count_hh = Math.floor(hh/3);
var show_hh = [];
for(var i=0;i<count_hh;i++){
	show_hh.push(i*3);
}
date_init();
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
function date_init(){
    var date = choose_time_for_mode();
    date.setHours(0,0,0,0);
    var max_date = date.format('yyyy/MM/dd');
    var current_date = date.format('yyyy/MM/dd');//获取当前日期，改格式
    var from_date_time = Math.floor(date.getTime()/1000) - 60*60*24;
    var min_date_ms = new Date()
    min_date_ms.setTime(from_date_time*1000);
    var from_date = min_date_ms.format('yyyy/MM/dd');
    if(global_test_mode==0){
        $('#detect_time_choose #weibo_from').datetimepicker({value:from_date,step:1440,format:'Y/m/d',timepicker:false});
        $('#detect_time_choose #weibo_to').datetimepicker({value:from_date,step:1440,format:'Y/m/d',timepicker:false});
        $('#detect_time_choose_modal #weibo_from_modal').datetimepicker({value:from_date,step:1440,format:'Y/m/d',timepicker:false});
        $('#detect_time_choose_modal #weibo_to_modal').datetimepicker({value:from_date,step:1440,format:'Y/m/d',timepicker:false});
        $('#search_date #weibo_modal').datetimepicker({value:from_date,step:1440,format:'Y/m/d',timepicker:false});
    }else{
        $('#detect_time_choose #weibo_from').datetimepicker({value:from_date,step:1440,minDate:'-1970/01/30',format:'Y/m/d',timepicker:false,maxDate:'+1970/01/01'});
        $('#detect_time_choose #weibo_to').datetimepicker({value:from_date,step:1440,minDate:'-1970/01/30',format:'Y/m/d',timepicker:false,maxDate:'+1970/01/01'});
        $('#detect_time_choose_modal #weibo_from_modal').datetimepicker({value:from_date,step:1440,format:'Y/m/d',timepicker:false});
        $('#detect_time_choose_modal #weibo_to_modal').datetimepicker({value:from_date,step:1440,format:'Y/m/d',timepicker:false});
        $('#search_date #weibo_modal').datetimepicker({value:from_date,step:1440,format:'Y/m/d',timepicker:false});

    }
    var real_date = new Date();
    real_date = real_date.format('yyyy/MM/dd');
    console.log(real_date);
    $('#search_date #weibo_modal').datetimepicker({value:real_date,step:1440,format:'Y/m/d',timepicker:false});

}
function submit_detect(){
	console.log('asdsfsfsd');
    var s = [];
    var keyword = $('#keyword_detect').val();
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
    }
}

$(function(){
    var url = '/network/show_daily_rank/'
    call_sync_ajax_request(url, temporal_rank_table);
});
//画表
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
	html += '<th style="text-align:center;">pagerank</th></thead>';
	
	for(var i=0;i<data.length;i++){
		var uid = data[i][0];
		var uname = data[i][1];
		if(uname == ''){
			uname = '未知';
		}
		var sign_loca = data[i][3];
		if(sign_loca == ''){
			sign_loca = '未知'
		}
		if(data[i][6]==1){ //是否入库
			var ifin = '是';
		}else{
			var ifin = '否';
		}
		if(data[i][4]==''){//fans
			data[i][4]= '未知';
		}
		if(data[i][5]==''){//pagerank
			data[i][5]= '未知';
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
		html += '</tr>';
	}
	html += '</table>';
	$('#result_rank_table').append(html);
	$('#rank_table').dataTable({
		"sDom": "<'row'<'col-md-6'l ><'col-md-6'f>r>t<'row'<'col-md-12'i><'col-md-12 center-block'p>>",
		"sPaginationType": "bootstrap",
		//"aoColumnDefs":[ {"bSortable": false, "aTargets":[1]}],
		"oLanguage": {
			"sLengthMenu": "每页 _MENU_ 条 ",
		}
	});
};

var url = '/network/show_daily_trend';
call_sync_ajax_request(url, show_trend);
//节点趋势图
function show_trend(data){
	var daily_data = [];
	for(var j=1;j<count_hh+1;j++){
        var period = 'period_'+j;
        if(data[period]){
			daily_data.push(data[period]);
		}else{
			daily_data.push(0);
		}
		
	}
	$(function () {
		$('#Activezh').highcharts({
			title: {
				text: '节点趋势图',
				x: -20 //center
			},
			xAxis: {
				categories: show_hh
			},
			yAxis: {
				title: {
					text: '节点度 '
				},
				plotLines: [{
					value: 0,
					width: 1,
					color: '#808080'
				}]
			},
			legend: {
				layout: 'vertical',
				align: 'right',
				verticalAlign: 'middle',
				borderWidth: 0
			},
			series: [{
				name:show_day,
				data: daily_data
			}]
		});
	});
}



//提交监控
function submit_detect(){
    var s = [];

    var show_arg = $('#detect_choose_detail_2 option:selected').text();
    var keyword = $('#keyword_detect').val();

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
