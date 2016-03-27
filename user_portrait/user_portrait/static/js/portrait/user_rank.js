function call_sync_ajax_request(url, callback){
    $.ajax({
      url: url,
      type: 'GET',
      dataType: 'json',
      async: true,
      success:callback
    });
}

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

function task_status (data) {
	$('#task_status').empty();
	if(sort_scope == 'all_nolimit'){
		alert('all')

		//call_sync_ajax_request(url, draw_all_rank_table);
	}else{
		alert('库内')
		//call_sync_ajax_request(url, draw_rank_table);
	}
	var html = '';
	html += '<table class="table table-striped" style="margin-left:30px;width:900px;">';
	for(var i=0;i<data.length;i++){
		html += '<tr>';
		html += '<td style="width:200px;text-align:center;">'+data[i][0]+'</td>';
		html += '<td>'+data[i][1]+'</td>';
		html += '</tr>';
	}
	html += '</table>';
	$('#task_status').append(html);
}

function draw_all_rank_table(data){
	var data = data;
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
	html += '<th style="text-align:center;">影响力</th>';
	html += '<th style="text-align:center;">言论敏感度</th>';
	html += '</thead>';
	for(var i=0;i<data.length;i++){
		var uid = data[i].uid;
		var uname = data[i].uname;
		if(uname == 'unknown'){
			uname = uid
		}
		var is_warehousing = '';
		if(data[i].is_warehousing == true){
			is_warehousing = '是';
		}else{
			is_warehousing == '否'
		}
		var location = data[i].location;
		var fans = data[i].fansnum
		var weibo_count = data[i].weibo_num;
		var influcence = data[i].bci_day_last;
		if(influcence == null){
			influcence = 0;
		}
		var sensitive = data[i].sen_day_last;
		if(sensitive == null){
			sensitive = 0;
		}
		html += '<tr>';
		html += '<td style="text-align:center;">'+(i+1)+'</td>';
		html += '<td style="text-align:center;"><a href="/index/personal/?uid='+uid+'" target="_blank">'+uid+'</a></td>';

		html += '<td style="text-align:center;">'+uname+'</td>';
		html += '<td style="text-align:center;">'+is_warehousing+'</td>';
		html += '<td style="text-align:center;">'+location+'</td>';
		html += '<td style="text-align:center;">'+fans+'</td>';
		html += '<td style="text-align:center;">'+weibo_count+'</td>';
		html += '<td style="text-align:center;">'+influcence.toFixed(2)+'</td>';
		html += '<td style="text-align:center;">'+sensitive.toFixed(2)+'</td>';
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
}

function draw_rank_table(data){
	if(data.length == 0){
		var html = '暂无数据';
		$('#result_rank_table').append(html);

	}else{
		$('#result_rank_table').empty();
		var html = '';
		html += '<table id="rank_table" class="table table-striped table-bordered bootstrap-datatable datatable responsive" style="margin-left:30px;width:900px;">';
		html += '<thead><th style="text-align:center;">排名</th>';
		html += '<th style="text-align:center;">用户ID</th>';
		html += '<th style="text-align:center;">昵称</th>';
		html += '<th style="text-align:center;">注册地</th>';
		html += '<th style="text-align:center;">领域</th>';//其实是话题
		html += '<th style="text-align:center;">身份</th>';//其实是领域
		html += '<th style="text-align:center;">身份敏感度</th>';
		html += '<th style="text-align:center;">活跃度</th>';
		html += '<th style="text-align:center;">影响力</th>';
		html += '<th style="text-align:center;">言论敏感度</th></thead>';
		for(var i=0;i<data.length;i++){

			var uid = data[i].uid;
			var uname = data[i].uname;
			if(uname == 'unknown'){
				uname = uid
			}
			var location = data[i].location;
			if(location == 'unknown'){
				location = '未知'
			}
			var topic = [];
			console.log(data[i].topic);
			//topic = data[i].topic.split('&');
			var domain = data[i].domain;
			var imp = data[i].imp;
			if(imp == null){
				imp = 0;
			}
			var active = data[i].act;
			if(active == null){
				active = 0;
			}

			//var weibo_count = data[i].weibo_num;
			var influcence = data[i].bci;
			if(influcence == null){
				influcence = 0;
			}
			var sensi = data[i].sen;
			if(data[i].sen == null){
				sensi = 0;
			}

			html += '<tr>';
			html += '<td style="text-align:center;">'+(i+1)+'</td>';
			html += '<td style="text-align:center;"><a href="/index/personal/?uid='+uid+'" target="_blank">'+uid+'</a></td>';
			html += '<td style="text-align:center;">'+uname+'</td>';
			html += '<td style="text-align:center;">'+location+'</td>';
			html += '<td style="text-align:center;">'+topic+'</td>';
			html += '<td style="text-align:center;">'+domain+'</td>';
			html += '<td style="text-align:center;">'+imp.toFixed(2) +'</td>';
			html += '<td style="text-align:center;">'+active.toFixed(2)+'</td>';
			html += '<td style="text-align:center;">'+influcence.toFixed(2)+'</td>';
			html += '<td style="text-align:center;">'+sensi.toFixed(2)+'</td>';
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
	}
}


//排序范围选择
$('#range_choose').change(function(){
	$('#range_choose_detail').empty();
	//库内-不限
	if($('#range_choose').val() == 'in_nolimit') {
		$('#sort_select').empty();
		var sort_select = '';
		sort_select += '<select  id="sort_select_2">';
		sort_select += '<option value="imp">身份敏感度</option>';
		sort_select += '<option value="act">活跃度</option>';
		sort_select += '<option value="bci">影响力</option>';
		sort_select += '<option value="ses">言论敏感度</option>';
		sort_select += '<option value="im_change">突发重要度变动</option>';
		sort_select += '<option value="acr_change">突发活跃度变动</option>';
		sort_select += '<option value="bci_change">突发影响力变动</option>';
		sort_select += '<option value="ses_change">突发敏感度变动</option>';
		sort_select += '</select>';
		$('#sort_select').append(sort_select);

		$('#time_choose').empty();
	    var time_html = '';	    
		time_html += '<input name="time_range" type="radio" value="1" checked="checked"> 过去一天';
		time_html += '<input name="time_range" type="radio" value="7" style="margin-left:20px;"> 过去七天';
		time_html += '<input name="time_range" type="radio" value="30" style="margin-left:20px;"> 过去一个月';
		$('#time_choose').append(time_html);
	}
	//库内-领域
	if($('#range_choose').val() == 'in_limit_domain') {
		$('#sort_select').empty();
		var sort_select = '';
		sort_select += '<select id="sort_select_2">';
		sort_select += '<option value="imp">身份敏感度</option>';
		sort_select += '<option value="act">活跃度</option>';
		sort_select += '<option value="bci">影响力</option>';
		sort_select += '<option value="ses">言论敏感度</option>';
		sort_select += '<option value="im_change">突发重要度变动</option>';
		sort_select += '<option value="acr_change">突发活跃度变动</option>';
		sort_select += '<option value="bci_change">突发影响力变动</option>';
		sort_select += '<option value="ses_change">突发敏感度变动</option>';
		sort_select += '</select>';
		$('#sort_select').append(sort_select);

		$('#time_choose').empty();
	    var time_html = '';	    
		time_html += '<input name="time_range" type="radio" value="1" checked="checked"> 过去一天';
		time_html += '<input name="time_range" type="radio" value="7" style="margin-left:20px;"> 过去七天';
		time_html += '<input name="time_range" type="radio" value="30" style="margin-left:20px;"> 过去一个月';
		$('#time_choose').append(time_html);

		var html = '';
		html += '<select id="range_choose_detail_2">';
		html += '<option value="">境内机构</option>'
		html += '<option value="">境外机构</option>'
		html += '<option value="">民间组织</option>'
		html += '<option value="">境外媒体</option>'
		html += '<option value="">活跃人士</option>'
		html += '<option value="">商业人士</option>'
		html += '<option value="">媒体人士</option>'
		html += '<option value="">高校</option>'
		html += '<option value="">草根</option>'
		html += '<option value="">媒体</option>'
		html += '<option value="">法律机构及人士</option>'
		html += '<option value="">政府机构及人士</option>'
		html += '<option value="">其他</option>'
		html += '</select>'
	};
	//库内-话题
	if($('#range_choose').val() == 'in_limit_topic') {
		$('#sort_select').empty();
		var sort_select = '';
		sort_select += '<select id="sort_select_2">';
		sort_select += '<option value="imp">身份敏感度</option>';
		sort_select += '<option value="act">活跃度</option>';
		sort_select += '<option value="bci">影响力</option>';
		sort_select += '<option value="ses">言论敏感度</option>';
		sort_select += '<option value="im_change">突发重要度变动</option>';
		sort_select += '<option value="acr_change">突发活跃度变动</option>';
		sort_select += '<option value="bci_change">突发影响力变动</option>';
		sort_select += '<option value="ses_change">突发敏感度变动</option>';
		sort_select += '</select>';
		$('#sort_select').append(sort_select);

		$('#time_choose').empty();
	    var time_html = '';	    
		time_html += '<input name="time_range" type="radio" value="1" checked="checked"> 过去一天';
		time_html += '<input name="time_range" type="radio" value="7" style="margin-left:20px;"> 过去七天';
		time_html += '<input name="time_range" type="radio" value="30" style="margin-left:20px;"> 过去一个月';
		$('#time_choose').append(time_html);

		var html = '';
		html += '<select id="range_choose_detail_2">';
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
	//库内-关键词（修改时间范围）
	if($('#range_choose').val() == 'in_limit_keyword') {
		$('#sort_select').empty();
		var sort_select = '';
		sort_select += '<select id="sort_select_2">';
		sort_select += '<option value="imp">身份敏感度</option>';
		sort_select += '<option value="act">活跃度</option>';
		sort_select += '<option value="bci">影响力</option>';
		sort_select += '<option value="ses">言论敏感度</option>';
		sort_select += '<option value="im_change">突发重要度变动</option>';
		sort_select += '<option value="acr_change">突发活跃度变动</option>';
		sort_select += '<option value="bci_change">突发影响力变动</option>';
		sort_select += '<option value="ses_change">突发敏感度变动</option>';
		sort_select += '</select>';
		$('#sort_select').append(sort_select);

		$('#time_choose').empty();
	    var time_html = '';
	    time_html += '<input id="weibo_from" type="text" class="form-control" style="width:145px; display:inline-block;height:25px;">&nbsp;-&nbsp;';
		time_html += '<input id="weibo_to" type="text" class="form-control" style="width:145px; display:inline-block;height:25px">';	    
		$('#time_choose').append(time_html);
		date_init();

		var html = '';
	    html += '<input id="keyword_hashtag" type="text" class="form-control" style="width:275px;height:25px;" placeholder="请输入关键词，多个词用英文逗号分开">';
	};
	//hashtag库内
	if($('#range_choose').val() == 'in_limit_hashtag') {
		$('#sort_select').empty();
		var sort_select = '';
		sort_select += '<select id="sort_select_2">';
		sort_select += '<option value="imp">身份敏感度</option>';
		sort_select += '<option value="act">活跃度</option>';
		sort_select += '<option value="bci">影响力</option>';
		sort_select += '<option value="ses">言论敏感度</option>';
		sort_select += '<option value="im_change">突发重要度变动</option>';
		sort_select += '<option value="acr_change">突发活跃度变动</option>';
		sort_select += '<option value="bci_change">突发影响力变动</option>';
		sort_select += '<option value="ses_change">突发敏感度变动</option>';
		sort_select += '</select>';
		$('#sort_select').append(sort_select);

		$('#time_choose').empty();
	    var time_html = '';
	    time_html += '<input id="weibo_from" type="text" class="form-control" style="width:145px; display:inline-block;height:25px;">&nbsp;-&nbsp;';
		time_html += '<input id="weibo_to" type="text" class="form-control" style="width:145px; display:inline-block;height:25px">';	    
		$('#time_choose').append(time_html);
		date_init();

		var html = '';
	    html += '<input id="keyword_hashtag" type="text" class="form-control" style="width:275px;height:25px;" placeholder="请输入微话题，多个话题用英文逗号分隔">';
	};
	//地理位置-库内
	if($('#range_choose').val() == 'in_limit_geo') {
		$('#sort_select').empty();
		var sort_select = '';
		sort_select += '<select id="sort_select_2">';
		sort_select += '<option value="imp">身份敏感度</option>';
		sort_select += '<option value="act">活跃度</option>';
		sort_select += '<option value="bci">影响力</option>';
		sort_select += '<option value="ses">言论敏感度</option>';
		sort_select += '<option value="im_change">突发重要度变动</option>';
		sort_select += '<option value="acr_change">突发活跃度变动</option>';
		sort_select += '<option value="bci_change">突发影响力变动</option>';
		sort_select += '<option value="ses_change">突发敏感度变动</option>';
		sort_select += '</select>';
		$('#sort_select').append(sort_select);

		$('#time_choose').empty();
	    var time_html = '';	    
		time_html += '<input name="time_range" type="radio" value="1" checked="checked"> 过去一天';
		time_html += '<input name="time_range" type="radio" value="7" style="margin-left:20px;"> 过去七天';
		time_html += '<input name="time_range" type="radio" value="30" style="margin-left:20px;"> 过去一个月';
		$('#time_choose').append(time_html);

		var html = '';
		html += '<select id="range_choose_detail_2">';
		html += '<option value="北京">北京</option>';
		html += '<option value="天津">天津</option>';
		html += '<option value="上海">上海</option>';
		html += '<option value="重庆">重庆</option>';
		html += '<option value="广东">广东省</option>';
		html += '<option value="浙江">浙江省</option>';
		html += '<option value="江苏">江苏省</option>';
		html += '<option value="福建">福建省</option>';
		html += '<option value="湖南">湖南省</option>';
		html += '<option value="湖北">湖北省</option>';
		html += '<option value="山东">山东省</option>';
		html += '<option value="辽宁">辽宁省</option>';
		html += '<option value="吉林">吉林省</option>';
		html += '<option value="云南">云南省</option>';
		html += '<option value="四川">四川省</option>';
		html += '<option value="安徽">安徽省</option>';
		html += '<option value="江西">江西省</option>';
		html += '<option value="黑龙江">黑龙江省</option>';
		html += '<option value="河北">河北省</option>';
		html += '<option value="陕西">陕西省</option>';
		html += '<option value="海南">海南省</option>';
		html += '<option value="河南">河南省</option>';
		html += '<option value="山西">山西省</option>';
		html += '<option value="内蒙古">内蒙古</option>';
		html += '<option value="广西">广西</option>';
		html += '<option value="贵州">贵州省</option>';
		html += '<option value="宁夏">宁夏</option>';
		html += '<option value="青海">青海省</option>';
		html += '<option value="新疆">新疆</option>';
		html += '<option value="西藏">西藏</option>';
		html += '<option value="甘肃">甘肃省</option>';
		html += '<option value="台湾">台湾省</option>';
		html += '<option value="香港">香港</option>';
		html += '<option value="澳门">澳门</option>';
		//html += '<option value="海外">海外</option>';
		html += '</select>';
	};
	//全网-all
	if($('#range_choose').val() == 'all_nolimit') {
		$('#sort_select').empty();
		var sort_select = '';
		sort_select += '<select id="sort_select_2">';
		sort_select += '<option value="fans">粉丝数</option>';
		sort_select += '<option value="weibo_count">发帖数</option>';
		sort_select += '<option value="bci">身份敏感度</option>';
		sort_select += '<option value="ses">言论敏感度</option>';
		sort_select += '<option value="bci_change">突发影响力变动</option>';
		sort_select += '<option value="ses_change">突发敏感度变动</option>';
		sort_select += '</select>';
		$('#sort_select').append(sort_select);

	    $('#time_choose').empty();
	    var time_html = '';	    
		time_html += '<input name="time_range" type="radio" value="1" checked="checked"> 过去一天';
		time_html += '<input name="time_range" type="radio" value="7" style="margin-left:20px;"> 过去七天';
		time_html += '<input name="time_range" type="radio" value="30" style="margin-left:20px;"> 过去一个月';
		$('#time_choose').append(time_html);

	}
	//全网-关键词
	if($('#range_choose').val() == 'all_limit_keyword') {
		var html = '';
	    html += '<input id="keyword_hashtag" type="text" class="form-control" style="width:275px;height:25px;" placeholder="请输入关键词，多个词用英文逗号分开">';
	    $('#sort_select').empty();
		var sort_select = '';
		sort_select += '<select id="sort_select_2">';
		sort_select += '<option value="fans">粉丝数</option>';
		sort_select += '<option value="weibo_count">发帖数</option>';
		sort_select += '<option value="bci">影响力</option>';
		sort_select += '<option value="ses">言论敏感度</option>';
		sort_select += '<option value="bci_change">突发影响力变动</option>';
		sort_select += '<option value="ses_change">突发敏感度变动</option>';
		sort_select += '</select>';
		$('#sort_select').append(sort_select);

		$('#time_choose').empty();
	    var time_html = '';
	    time_html += '<input id="weibo_from" type="text" class="form-control" style="width:145px; display:inline-block;height:25px;">&nbsp;-&nbsp;';
		time_html += '<input id="weibo_to" type="text" class="form-control" style="width:145px; display:inline-block;height:25px">';	    
		$('#time_choose').append(time_html);
		date_init();

		var html = '';
	    html += '<input id="keyword_hashtag" type="text" class="form-control" style="width:275px;height:25px;" placeholder="请输入关键词，多个词用英文逗号分开">';

	};
	$('#range_choose_detail').append(html);
});

//筛选条件初始化时间
function date_init(){
	var date = choose_time_for_mode();
	console.log(date)
	date.setHours(0,0,0,0);
	var max_date = date.format('yyyy/MM/dd hh:mm');
	var current_date = date.format('yyyy/MM/dd hh:mm');
	var from_date_time = Math.floor(date.getTime()/1000) - 60*60*24;
	var min_date_ms = new Date()
	min_date_ms.setTime(from_date_time*1000);
	var from_date = min_date_ms.format('yyyy/MM/dd hh:mm');
	if(global_test_mode==0){
	    $('#time_choose #weibo_from').datetimepicker({value:from_date,step:60});
	    $('#time_choose #weibo_to').datetimepicker({value:current_date,step:60});
	}else{
	    $('#time_choose #weibo_from').datetimepicker({value:from_date,step:60,minDate:'-1970/01/30',maxDate:'+1970/01/01'});
	    $('#time_choose #weibo_to').datetimepicker({value:current_date,step:60,minDate:'-1970/01/30',maxDate:'+1970/01/01'});
	}
}

function submit_rank(){
	var s = [];
	var show_scope = $('#range_choose option:selected').text();
	var show_arg = $('#range_choose_detail_2 option:selected').text();
	var show_norm = $('#sort_select_2 option:selected').text();
	var keyword = $('#keyword_hashtag').val();
	var sort_scope = $('#range_choose option:selected').val();
	var sort_norm = $('#sort_select_2 option:selected').val();
	var arg = $('#range_choose_detail_2 option:selected').text();
	var day_select = $("input[name='time_range']:checked").val();
	//console.log(keyword);
	if(keyword == ''){  //检查输入词是否为空
		alert('请输入关键词！');
	}else{
		if(keyword == undefined){  //没有输入的时候，更新表格及文字
			var url = '/user_rank/user_sort/?&username='+username+'&time='+day_select+'&sort_norm='+sort_norm+'&sort_scope='+sort_scope;
			var data = [['111关键词：两会','111状态：正在计算'],['关键词：两会','状态：正在计算'],['关键词：两会','状态：正在计算']];
			//task_status(data);
			draw_rank_table(data);
			$('#rec_range').empty();
			$('#rec_detail').empty();
			$('#rec_rank_by').empty();
			$('#rec_time_range').empty();
			$('#rec_range').append(show_scope);
			if(sort_scope != 'in_nolimit' && sort_scope != 'all_nolimit' ){  // 参数是可选的时候，加上详细条件
				$('#rec_detail').append('-');
				$('#rec_detail').append(show_arg);
				url += '&arg='+arg;   //该参数为空时不传
			}
			// if(keyword != undefined){
			// 	$('#rec_detail').append('：');
			// 	$('#rec_detail').append(keyword);
			// }
			$('#rec_rank_by').append(show_norm);
			if(day_select == "1"){
				$('#rec_time_range').append('过去一天');
			}
			if(day_select == "7"){
				$('#rec_time_range').append('过去七天');
			}
			if(day_select == "30"){
				$('#rec_time_range').append('过去一个月');
			}
			if(sort_scope == 'all_nolimit'){
				call_sync_ajax_request(url, draw_all_rank_table);
			}else{
				alert('库内')
				//call_sync_ajax_request(url, draw_rank_table);
			}
			console.log(url);
		}else{ //输入参数的时候，更新任务状态表格
			var keyword_array = [];
			var keyword_array = keyword.split(',');
			var keyword_string = keyword_array.join(',');
			var time_from = user_rank_timepicker($('#time_choose #weibo_from').val());
			console.log(time_from)
			var time_to = user_rank_timepicker($('#time_choose #weibo_to').val());
			var time_from_after = new Date(time_from*1000)
			var time_to_after = new Date(time_to*1000)
			time_from_after = time_from_after.format('yyyy-MM-dd')
			time_to_after = time_to_after.format('yyyy-MM-dd')
			console.log(time_from_after)
			var url = '/user_rank/user_sort/?time=-1&username='+username+'&st='+time_from_after +'&et='+time_to_after+'&sort_norm='+sort_norm+'&sort_scope='+sort_scope+'&arg='+keyword;
			var data = [['121关键词：两会','121状态：正在计算'],['关键词：两会','状态：正在计算'],['关键词：两会','状态：正在计算']];
			var task_url = '/user_rank/search_task/?username='+username;
			task_status(data);
			console.log(url);
		}
	}
}

//结果分析默认值
var username = $('#username').text();
var sort_scope = $('#range_choose option:selected').val();
var sort_norm = $('#sort_select_2 option:selected').val();
var arg = $('#range_choose_detail_2 option:selected').text();
var day_select = $("input[name='time_range']:checked").val();
$('#rec_range').append($('#range_choose option:selected').text());
// $('#rec_detail').append('：');
// $('#rec_detail').append($('#range_choose option:selected').text());
$('#rec_rank_by').append($('#sort_select_2 option:selected').text());
var day_select = $("input[name='time_range']:checked").val();
if(day_select == "1"){
	$('#rec_time_range').append('过去一天')
}
if(day_select == "7"){
	$('#rec_time_range').append('过去七天')
}
if(day_select == "30"){
	$('#rec_time_range').append('过去三十天')
}

var data = [['关键词：两会','状态：正在计算'],['关键词：两会','状态：正在计算'],['关键词：两会','状态：正在计算']];
//默认时间是？？
//var task_url = '/user_rank/search_task/?time=-1&username='+username+'&st='+time_from_after +'&et='+time_to_after+'&sort_norm='+sort_norm+'&sort_scope='+sort_scope+'&arg='+keyword;
var rank_url = '/user_rank/user_sort/?username='+username+'&time='+day_select+'&sort_norm='+sort_norm+'&sort_scope='+sort_scope;
console.log(rank_url);
call_sync_ajax_request(rank_url, draw_all_rank_table)
task_status(data);
//draw_rank_table(data);
