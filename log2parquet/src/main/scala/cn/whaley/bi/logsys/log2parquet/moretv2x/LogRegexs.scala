package cn.whaley.bi.logsys.log2parquet.moretv2x

/**
 * Created by Will on 2015/9/19.
 * This object contains a collect of regex, which is for one special situation.
 */
object LogRegexs {

    val regexPlayqos = ( "(\\d+)-" +
            "play-001-(userexit|playend|playerror|sourceerror)-(MoreTV[\\w\\.]+)-" +
            "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(.+)-(\\w+)-" +
            "(xiqu|mv|sports|movie|tv|zongyi|comic|kids|jilu|hot)-" +
            "([a-z0-9]{12})-([a-z0-9]{12})-(\\d+)-(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexPlayview = ( "(\\d+)-" +
        "play-001-(playview)-(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(.+)-" +
            "(xiqu|mv|sports|movie|tv|zongyi|comic|kids|jilu|hot)-([a-z0-9]{12})-([a-z0-9]{12})-" +
            "(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexPlayBaiduCloud = ( "(\\d+)-" +
        "play-001-(playview|userexit|playend|playerror|sourceerror)-" +
            "(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(home-baidu_cloud)-(.+)-" +
            "(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexDetail = ( "(\\d+)-" +
        "detail-001-(view)-(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(.+)-" +
            "(movie|tv|zongyi|kids|comic|jilu)-([a-z0-9]{12})-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexDetailSubject = ( "(\\d+)-" +
        "detail-001-(view)-(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(.+)-" +
            "(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+|xiqu\\d+|mv\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexCollect = ( "(\\d+)-" +
        "collect-001-(ok|cancel)-(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(\\w+)-(.+?)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r


    val regexOperationShowlivelist = ( "(\\d+)-" +
        "operation-001-(showlivelist)-" +
            "(MoreTV[\\w\\.]+)-"
            + "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(\\w+)-(\\d*)-(.+?)-(\\d{14})&sid=([a-z0-9]{12})").r

    val regexOperationTimeshifting = ( "(\\d+)-" +
        "operation-001-(timeshifting)-" +
            "(MoreTV[\\w\\.]+)-"
            + "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(\\w+)-(\\d*)-(.+?)-(\\d{14})-sid\\*([a-z0-9]{12})").r

    val regexOperationEvaluate = ( "(\\d+)-" +
        "operation-001-(evaluate)-" +
            "(MoreTV[\\w\\.]+)-"
            + "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(\\w+)-(\\d*)-(.+?)-(\\d{14})-([a-z0-9]{12})-(\\w+)-(up|down)").r

    //This was for one which event was moretag or multiseason
    val regexOperationMM = ( "(\\d+)-" +
        "operation-001-(moretag|multiseason)-" +
            "(MoreTV[\\w\\.]+)-"
            + "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    //This was for one which event was addtag, comment or watchprevue
    val regexOperationACW = ( "(\\d+)-" +
        "operation-001-(addtag|comment|watchprevue|notrecommend)-" +
            "(MoreTV[\\w\\.]+)-"
            + "([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-(\\w+)-(\\d*)-(.+?)-(\\d{14})-(\\w+)-([a-z0-9]{12})").r

    val regexLive = ( "(\\d+)-" +
        "live-001-(MoreTV[\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})-" +
            "([\\w\\-]+)-(\\d{1})-([a-z0-9]{12})-(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexInterviewEnter = ( "(\\d+)-" +
        "interview-001-enter-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(.+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexInterviewExit = ( "(\\d+)-" +
        "interview-001-exit-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(.+)-(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexApprecommend = ( "(\\d+)-" +
        "apprecommend-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-([a-z0-9]{12}-app\\d+|[a-z0-9]{12}-|[a-z0-9]{12})-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexEnter = ( "(\\d+)-" +
        "enter-001-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "(-thirdparty_\\d{1})?-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexExit = ( "(\\d+)-" +
        "exit-001-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "(-thirdparty_\\d{1})?-(\\d+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexHomeaccess = ( "(\\d+)-" +
        "homeaccess-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})&location=(\\d+)_(\\d+)").r

    val regexMtvaccount = ( "(\\d+)-" +
        "mtvaccount-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexMtvaccountLogin = ( "(\\d+)-" +
        "mtvaccount-001-(login)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-path\\*(\\w+)").r

    val regexPageview = ( "(\\d+)-" +
        "pageview-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})(\\S*)").r

    val regexPositionaccess = ( "(\\d+)-" +
        "positionaccess-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-(\\d+)_(\\d+)").r

    val regexAppsubject = ( "(\\d+)-" +
        "appsubject-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-(\\w+)").r

    val regexStarpage = ( "(\\d+)-" +
        "starpage-001-(\\w+)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-star\\*([^\\-]+)-path\\*-([\\w\\-]+)").r

    val regexRetrieval = ( "(\\d+)-" +
        "retrieval-001-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(.+)-(\\w+)-(\\w+)-(\\w+)-(\\w+)-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexSetWallpaper = ( "(\\d+)-" +
        "set-001-(wallpaper)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})&wallpaper=(\\d+)").r

    val regexHomeRecommend = ( "(\\d+)-" +
        "homerecommend-001-(access)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*|[a-f0-9]{32}-\\d*|[a-f0-9]{32})" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-([^-]+)-([^-]+)-([^-]+)").r

    val regexHomeviewEnter = ( "(\\d+)-" +
        "homeview-001-enter-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*)" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexHomeviewExit = ( "(\\d+)-" +
        "homeview-001-exit-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*)" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-(\\d+)").r

    val regexDanmustatus = ( "(\\d+)-" +
        "danmustatus-001-(on|off)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*)" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})").r

    val regexDanmuswitch = ( "(\\d+)-" +
        "danmuswitch-001-(on|off)-([\\w\\.]+)-([a-f0-9]{32}-\\d*-\\d*)" +
            "-(\\w+)-(\\d*)-(.+?)-(\\d{14})-([a-z0-9]{12})-(\\w+)").r

}
