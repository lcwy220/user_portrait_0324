
//日期选择
var date = choose_time_for_mode();
var min_date_ms = new Date()
date.setHours(0,0,0,0);
var current_date = date.format('yyyy/MM/dd hh:mm');
var from_date_time = Math.floor(date.getTime()/1000) - 60*60*24;
min_date_ms.setTime(from_date_time*1000);
var from_date = min_date_ms.format('yyyy/MM/dd hh:mm');
if (global_test_mode == 0){
    $('#attribute_pattern #weibo_from').datetimepicker({value:from_date,step:10});
    $('#attribute_pattern #weibo_to').datetimepicker({value:current_date,step:10});
}
else{
    $('#attribute_pattern #weibo_from').datetimepicker({value:from_date,step:10,minDate:'-1970/01/30',maxDate:'+1970/01/01'});
    $('#attribute_pattern #weibo_to').datetimepicker({value:current_date,step:10,minDate:'-1970/01/30',maxDate:'+1970/01/01'});
}    
    $('#attribute_pattern #time_checkbox').click(function(){
        if($(this).is(':checked')){
            $('#attribute_pattern #weibo_from').attr('disabled',false);
            $('#attribute_pattern #weibo_to').attr('disabled',false);
        }
        else{
            $('#attribute_pattern #weibo_from').attr('disabled',true);
            $('#attribute_pattern #weibo_to').attr('disabled',true);
        }
    });
  var time_from = Date.parse($('#attribute_pattern #weibo_from').val())/1000;
  var time_to = Date.parse($('#attribute_pattern #weibo_to').val())/1000;
  console.log('time_to', time_to);
      if(time_from > time_to){
        alert('起止时间错误，请重新选择！');
        return false;
      }
      if(max_date_limit_stamp < time_to ){
        alert('终止时间最晚不超过今日零点，请重新选择！');
        return false;
      }
      var geo_url = ''
      var geo_id = $('#attribute_pattern #geo').attr("id");
      var geo_content = $('#attribute_pattern #geo').val();
      if(geo_content != ''){
        geo_url = geo_id +'='+ geo_content;
        url_all.push(geo_url);
      }
      var ip_url = ''
      var ip_id = $('#attribute_pattern #ip').attr("id");
      var ip_content = $('#attribute_pattern #ip').val();
      if(ip_content != ''){
        ip_url = ip_id +'='+ ip_content;
        url_all.push(ip_url);
      }
      var message_url = '';
      var message_string = new Array();
      $('#attribute_pattern #message_type .inline-checkbox').each(function(){
        if($(this).is(':checked')){
          message_string.push($(this).next().text());
        }
      });
      if(message_string.length != 0){
        message_url += 'message_type=' + message_string.join(',');
        url_all.push(message_url);
      }
      var sentiment_url = '';
      var sentiment_string = new Array();
      $('#attribute_pattern #sentiment .inline-checkbox').each(function(){
        if($(this).is(':checked')){
          sentiment_string.push($(this).next().text());
        }
      });
      if(sentiment_string.length != 0){
        sentiment_url += 'sentiment=' + sentiment_string.join(',');
        url_all.push(sentiment_url);
      }
      if ($('#attribute_pattern #time_checkbox').is(':checked')){
          var time_from = Date.parse($('#attribute_pattern #weibo_from').val())/1000;
          var time_to = Date.parse($('#attribute_pattern #weibo_to').val())/1000;
          var timestamp_from_url = '';
          timestamp_from_url = 'timestamp_from=' + time_from;
          url_all.push(timestamp_from_url);
          var timestamp_to_url = '';
          timestamp_to_url = 'timestamp_to=' + time_to;
          url_all.push(timestamp_to_url);
      }
