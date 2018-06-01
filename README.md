# 什么是livego<br/>     
livego是基于golang开发的rtmp服务器<br/>
协议部分的核心代码，很多来源于: https://github.com/sheepbao/sms/tree/master/src/vendor/sheepbao.com/media
<br/>     
# 为什么基于golang
*  ## golang在语言基本支持多核CPU均衡使用，支持海量轻量级线程，提高其并发量<br/>   
   当前开源的缺陷：
   - srs只能运行在一个单核下，如果需要多核运行，只能启动多个srs监听不同的端口来提高并发量；<br/>   
   - ngx-rtmp启动多进程后，报文在多个进程内转发，需要二次开发，否则静态推送到多个子进程，效能消耗大；<br/>   
   golang在语言级别解决了上面多进程并发的问题。
*  ## 二次开发简洁快速<br/>   
   golang的开发效率远远高过C/C++

# livego支持哪些特性<br/>     
*  rtmp 推流，拉流
*  支持hls观看
*  支持http-flv观看
*  支持gop-cache缓存
*  静态relay支持：支持静态推流，拉流
*  统计信息支持：支持http在线查看流状态
*  支持直播云导播台功能

## rtmp配置指引
livego的rtmp配置是基于json格式，简单好用。<br/> 
{<br/> 
    "listen": 1935,<br/> 
    "hls": "enable",<br/> 
    "servers":[<br/> 
        {<br/> 
        "servername":"live"<br/> 
        }<br/> 
    ]<br/> 
}<br/> 

## wiki指导
wiki地址：[https://github.com/runner365/livego/wiki](https://github.com/runner365/livego/wiki)
