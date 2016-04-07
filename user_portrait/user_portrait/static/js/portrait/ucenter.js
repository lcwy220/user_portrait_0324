Date.prototype.format = function(format){
    var o = {
        "M+" : this.getMonth()+1, //month
        "d+" : this.getDate(), //day
        "h+" : this.getHours(), //hour
        "m+" : this.getMinutes(), //minute
        "s+" : this.getSeconds(), //second
        "q+" : Math.floor((this.getMonth()+3)/3), //quarter
        "S" : this.getMilliseconds() //millisecond
    }
    if(/(y+)/.test(format)){
        format=format.replace(RegExp.$1, (this.getFullYear()+"").substr(4 - RegExp.$1.length));
    }
    for(var k in o){
        if(new RegExp("("+ k +")").test(format)){
            format = format.replace(RegExp.$1, RegExp.$1.length==1 ? o[k] : ("00"+ o[k]).substr((""+ o[k]).length));
        }
    }
    return format;
}

function call_sync_ajax_request(url, callback){
    $.ajax({
      url: url,
      type: 'GET',
      dataType: 'json',
      async: true,
      success:callback
    });
}

function modal_work(){
	$('#Worktable').empty();
	var date = new Date();
	var to_date = new Date();
	to_date.setTime(date.getTime() - 60*60*24*7*1000);
	console.log(to_date);
	var from_date = date.format('yyyy/MM/dd');
	to_date = to_date.format('yyyy/MM/dd');
	// var work_name = ['用户排行', '情绪监测', '网络分析', '入库推荐', '群体发现', '群体分析', '社会感知' ]
	var dict_name = {'用户排行':'rank_task', '情绪监测':'sentiment_task', '网络分析':'network_task', '入库推荐':'recomment', '群体发现':'group_detect', '群体分析':'group_analysis', '社会感知':'sensing_task'};
	var html = '';
	html += ' <table class="table table-bordered table-striped table-condensed datatable" >';
	html += ' <thead><tr style="text-align:center;">';
	html += '<th>任务ID</th><th>任务内容</th><th>提交时间</th><th>处理状态</th>';
	html += '</tr></thead>';
	html += '<tbody>';
	for(key in dict_name){
		html += '<tr>'
		html += '<td>'+'09XPX78'+'</td>';
		html += '<td>'+key+'</td>';
		html += '<td>'+from_date+' - '+to_date+'</td>';
		html += '<td><span hidden>'+dict_name[key]+'</span><u style="cursor:pointer;" class="detail_button" type="button" data-toggle="modal" data-target="#'+dict_name[key]+'_detail">查看详情</u></td>';
		// }
		html += '</tr>';
	}
	// for(var i=0;i<work_name.length;i++){
	// 	html += '<tr>'
	// 	html += ' <td>'+'09XPX78'+'</td>';
	// 	html += ' <td>'+work_name[i]+'</td>';
	// 	html += ' <td>'+from_date+'-'+to_date+'</td>';
	// 	html += '<td style="cursor:pointer;" id="detail_button" type="button" data-toggle="modal" data-target="#detail_in_portrait"><u>查看详情</u></td>';
	// 	// }
	// 	html += '</tr>';
	// }
	html += '</tbody></table>';
	$('#Worktable').append(html);
}

function Draw_recomment_modal(data){

	$('#recomment_detail_modal').empty();
	var html = '';
	html += ' <table class="table table-bordered table-striped table-condensed datatable" >';
	html += ' <thead><tr style="text-align:center;">';
	// for(var i=0; i<title_list; i++){
	// 	html += '<th>' + title_list[i] +'</th>';
	// }
	html += '<th>日期</th><th>uid</th><th>昵称</th><th>地理位置</th><th>粉丝数</th><th>微博数</th><th>影响力</th><th>是否入库</th>';
	html += '</tr></thead>';
	html += '<tbody>';
	for(var i=0;i<data.length;i++){
		html += '<tr>';
		html += '<td style="text-align;">'+data[i][0]+'</td>'
		html += '<td style="text-align;">'+data[i][1]+'</td>'
		html += '<td style="text-align;">'+data[i][2]+'</td>'
		html += '<td style="text-align;">'+data[i][3]+'</td>'
		html += '<td style="text-align;">'+data[i][4]+'</td>'
		html += '<td style="text-align;">'+data[i][5]+'</td>'
		html += '<td style="text-align;">'+data[i][6].toFixed(2)+'</td>';
		if(data[i][7] == '1'){
			html += '<td style="text-align;">'+'是'+'</td>';
		}else{
			html += '<td style="text-align;">'+'否'+'</td>';
		}
		html += '</tr>'
	}
	html += '</tbody></table>';
	$('#recomment_detail_modal').append(html);
}

function Draw_rank_task_modal(data){
	// console.log(data);
	$('#rank_task_detail_modal').empty();
	var html = '';
	html += ' <table class="table table-bordered table-striped table-condensed datatable" >';
	html += ' <thead><tr style="text-align:center;">';
	// for(var i=0; i<title_list; i++){
	// 	html += '<th>' + title_list[i] +'</th>';
	// }
	html += '<th>关键词</th><th>排序范围</th><th>排序指标</th><th>提交时间</th><th>计算状态</th>';
	html += '</tr></thead>';
	html += '<tbody>';
	for(var i=0;i<data.length;i++){
		html += '<tr>';
		html += '<td style="text-align;">'+data[i][0].split('&').join(' ')+'</td>'
		html += '<td style="text-align;">'+data[i][0]+'</td>'
		html += '<td style="text-align;">'+norm_dict[data[i][1]]+'</td>'
		html += '<td style="text-align;">'+data[i][2]+'</td>'
		if(data[i][3] == '1'){
			html += '<td style="text-align;">'+'计算完成'+'</td>';
		}else{
			html += '<td style="text-align;">'+'正在计算'+'</td>';
		}
		html += '</tr>'
	}
	html += '</tbody></table>';
	$('#rank_task_detail_modal').append(html);
}

function Draw_sentiment_task_modal(data){

	$('#sentiment_task_detail_modal').empty();
	var html = '';
	html += ' <table class="table table-bordered table-striped table-condensed datatable" >';
	html += ' <thead><tr style="text-align:center;">';
	// for(var i=0; i<title_list; i++){
	// 	html += '<th>' + title_list[i] +'</th>';
	// }
	html += '<th>关键词</th><th>时间范围</th><th>提交时间</th><th>计算状态</th>';
	html += '</tr></thead>';
	html += '<tbody>';
	for(var i=0;i<data.length;i++){
		html += '<tr>';
		html += '<td style="text-align;">'+data[i][1].split('&').join(' ')+'</td>'
		html += '<td style="text-align;">'+data[i][2]+' 至 '+data[i][3]+'</td>'
		// html += '<td style="text-align;">'+data[i][2]+'</td>'
		// html += '<td style="text-align;">'+data[i][3]+'</td>'
		html += '<td style="text-align;">'+data[i][4]+'</td>'
		if(data[i][5] == '1'){
			html += '<td style="text-align;">'+'计算完成'+'</td>';
		}else{
			html += '<td style="text-align;">'+'正在计算'+'</td>';
		}
		html += '</tr>'
	}
	html += '</tbody></table>';
	$('#sentiment_task_detail_modal').append(html);
}

function Draw_network_task_modal(data){
	$('#network_task_detail_modal').empty();
	var html = '';
	html += ' <table class="table table-bordered table-striped table-condensed datatable" >';
	html += ' <thead><tr style="text-align:center;">';
	// for(var i=0; i<title_list; i++){
	// 	html += '<th>' + title_list[i] +'</th>';
	// }
	html += '<th>关键词</th><th>时间范围</th><th>提交时间</th><th>计算状态</th>';
	html += '</tr></thead>';
	html += '<tbody>';
	for(var i=0;i<data.length;i++){
		html += '<tr>';
		html += '<td style="text-align;">'+data[i][1].split('&').join(' ')+'</td>'
		html += '<td style="text-align;">'+data[i][3]+' 至 '+data[i][4]+'</td>'
		// html += '<td style="text-align;">'+data[i][2]+'</td>'
		// html += '<td style="text-align;">'+data[i][3]+'</td>'
		html += '<td style="text-align;">'+data[i][2]+'</td>'
		if(data[i][5] == '1'){
			html += '<td style="text-align;">'+'计算完成'+'</td>';
		}else{
			html += '<td style="text-align;">'+'正在计算'+'</td>';
		}
		html += '</tr>'
	}
	html += '</tbody></table>';
	$('#network_task_detail_modal').append(html);
}

function Draw_group_detect_modal(data){
	var task_dict = {'single':'由用户发现群体','multy':'由用户发现群体','attribute':'由特征发现群体','pattern': '由模式发现群体','event':'由事件发现群体'}
	$('#group_detect_detail_modal').empty();
	var html = '';
	html += ' <table class="table table-bordered table-striped table-condensed datatable" >';
	html += ' <thead><tr style="text-align:center;">';
	// for(var i=0; i<title_list; i++){
	// 	html += '<th>' + title_list[i] +'</th>';
	// }
	html += '<th>任务名称</th><th>任务类型</th><th>备注</th><th>提交时间</th><th>任务进度</th>';
	html += '</tr></thead>';
	html += '<tbody>';
	for(var i=0;i<data.length;i++){
		html += '<tr>';
		html += '<td style="text-align;">'+data[i][0]+'</td>'
		html += '<td style="text-align;">'+task_dict[data[i][3]]+'</td>'
		html += '<td style="text-align;">'+data[i][2]+'</td>'
		html += '<td style="text-align;">'+data[i][1]+'</td>'
		html += '<td style="text-align;"><progress value="'+data[i][4]+'" max="100"></progress>&nbsp;&nbsp;'+data[i][4]+'%</td>'
		// html += '<td style="text-align;">'+data[i][2]+'</td>'
		// html += '<td style="text-align;">'+data[i][3]+'</td>'
		// html += '<td style="text-align;">'+data[i][2]+'</td>'
		// if(data[i][5] == '1'){
		// 	html += '<td style="text-align;">'+'计算完成'+'</td>';
		// }else{
		// 	html += '<td style="text-align;">'+'正在计算'+'</td>';
		// }
		html += '</tr>'
	}
	html += '</tbody></table>';
	$('#group_detect_detail_modal').append(html);
}

function Draw_group_analysis_modal(data){
	var task_dict = {'single':'由用户发现群体','multy':'由用户发现群体','attribute':'由特征发现群体','pattern': '由模式发现群体','event':'由事件发现群体'}
	$('#group_analysis_detail_modal').empty();
	var html = '';
	html += ' <table class="table table-bordered table-striped table-condensed datatable" >';
	html += ' <thead><tr style="text-align:center;">';
	html += ' <th>任务名称</th><th>备注</th><th>提交时间</th><th>任务状态</th>';
	html += ' </tr></thead>';
	html += ' <tbody>';
	for(var i=0;i<data.length;i++){
		html += '<tr>';
		html += '<td style="text-align;">'+data[i][0]+'</td>'
		html += '<td style="text-align;">'+data[i][2]+'</td>'
		html += '<td style="text-align;">'+data[i][1]+'</td>'
		// html += '<td style="text-align;">'+data[i][2]+'</td>'
		// html += '<td style="text-align;">'+data[i][3]+'</td>'
		// html += '<td style="text-align;">'+data[i][2]+'</td>'
		if(data[i][3] == '1'){
			html += '<td style="text-align;">'+'计算完成'+'</td>';
		}else{
			html += '<td style="text-align;">'+'正在计算'+'</td>';
		}
		html += '</tr>'
	}
	html += '</tbody></table>';
	$('#group_analysis_detail_modal').append(html);
}

function Draw_sensing_task_modal(data){
	$('#sensing_task_detail_modal').empty();
	var html = '';
	html += ' <table class="table table-bordered table-striped table-condensed datatable" >';
	html += ' <thead><tr style="text-align:center;">';
	html += ' <th>任务名称</th><th>备注</th><th>提交时间</th><th>任务状态</th>';
	html += ' </tr></thead>';
	html += ' <tbody>';
	for(var i=0;i<data.length;i++){
		html += '<tr>';
		html += '<td style="text-align;">'+data[i][0]+'</td>'
		html += '<td style="text-align;">'+data[i][2]+'</td>'
		html += '<td style="text-align;">'+data[i][1]+'</td>'
		// html += '<td style="text-align;">'+data[i][2]+'</td>'
		// html += '<td style="text-align;">'+data[i][3]+'</td>'
		// html += '<td style="text-align;">'+data[i][2]+'</td>'
		if(data[i][3] == '1'){
			html += '<td style="text-align;">'+'计算完成'+'</td>';
		}else{
			html += '<td style="text-align;">'+'正在计算'+'</td>';
		}
		html += '</tr>'
	}
	html += '</tbody></table>';
	$('#sensing_task_detail_modal').append(html);
}


function modal_data(data){
	Draw_rank_task_modal(data.rank_task);
	Draw_sentiment_task_modal(data.sentiment_task);
	Draw_network_task_modal(data.network_task);
	Draw_group_detect_modal(data.group_detect);
	Draw_group_analysis_modal(data.group_analysis);
	Draw_sensing_task_modal(data.sensing_task);

	return data;
}

function modal_data_re(data){
	console.log(data)
	console.log(data.recomment)
	Draw_recomment_modal(data.recomment);

	return data;
}

var norm_dict ={'weibo_num': '微博数','fans': '粉丝数','bci': '影响力','bci_change':'突发影响力变动','ses':'言论敏感度','ses_change':'突发敏感度变动','imp':'身份敏感度','imp_change':'突发重要度变动','act':'活跃度','act_change':'突发活跃度变动'}
var dict_name = {'rank_task':'用户排行', 'sentiment_task':'情绪监测', 'network_task':'网络分析', 'recomment':'入库推荐','group_detect' :'群体发现', 'group_analysis':'群体分析', 'sensing_task':'社会感知'};
modal_work();
var admin=$('#tag_user').text();
var url_recomment = '/ucenter/user_operation/?submit_user='+ admin;
console.log(url_recomment)
call_sync_ajax_request(url_recomment, modal_data_re);
var url_else = '/ucenter/user_operation/?submit_user=admin';
call_sync_ajax_request(url_else, modal_data);


// $('.detail_button').click(function(){
// 	var data = modal_data();
// 	var data_re = modal_data_re();
// 	var work_name = $(this).prev().text();
// 	var title = dict_name[work_name];
// 	if(work_name == 'recomment'){
// 		Draw_modal(data_re[work_name], title)
// 		// Draw_modal(data[])
// 		// call_sync_ajax_request(url, function(data){Draw_modal(data, title, work_name)});
// 	}else{
// 		Draw_modal(data[work_name], title)
// 	}

// });


