import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by fj on 15/12/8.
 */
public class Utils {
    /**
     * 使用当前系统文件分隔符,将多个路径合并成一个末尾不包含文件分隔符的路径,期间自动处理前后路径是否存在分隔符的问题
     *
     * @param paths
     * @return
     */
    public static String joinPath(String... paths) {
        return joinPathBy(File.separator, paths);
    }

    /**
     * 使用指定文件分隔符,将多个路径合并成一个末尾不包含文件分隔符的路径,直接用文件分隔符链接,期间自动处理前后路径是否存在分隔符的问题
     *
     * @param separator
     * @param paths
     * @return
     */
    public static String joinPathBy(String separator, String... paths) {
        String all = paths[0];
        for (int i = 1; i < paths.length; i++) {
            if (paths[i] == null) continue;
            if (!all.endsWith(separator)) all = all + separator;
            if (paths[i].startsWith(separator)) {
                all = all + paths[i].substring(1);
            } else {
                all = all + paths[i];
            }
        }
        if (all.endsWith(separator)) return all.substring(0, all.length() - 1);
        return all;
    }

    /**
     * 将一系列字符串连接起来
     *
     * @param spilt 连接符
     * @param strs
     * @return
     */
    public static String concat(String spilt, String... strs) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < strs.length; i++) {
            if (strs[i] == null) continue;
            if (i > 0 && spilt != null) buffer.append(spilt);
            buffer.append(strs[i]);
        }
        return buffer.toString();
    }

    /**
     * 获取当前机器ip地址
     */
    public static String getNetworkAddress() {
        Enumeration<NetworkInterface> netInterfaces;
        try {
            netInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip;
            while (netInterfaces.hasMoreElements()) {
                NetworkInterface ni = netInterfaces
                        .nextElement();
                Enumeration<InetAddress> addresses=ni.getInetAddresses();
                while(addresses.hasMoreElements()){
                    ip = addresses.nextElement();
                    if (!ip.isLoopbackAddress() && ip.getHostAddress().indexOf(':') == -1) {
                        return ip.getHostAddress();
                    }
                }
            }
            return "";
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 获取当前进程ID
     * @return
     */
    public static Integer getCurrProcessId(){
        String name = ManagementFactory.getRuntimeMXBean().getName();
        Integer pid= Integer.parseInt(name.substring(0, name.indexOf("@")));
        return pid;
    }

    /**
     * 如果v为null,则返回elseV,且如果v为string实例,则进一步判断v是否为空字符串或只包含了空格的字符串
     *
     * @param v
     * @param elseV
     * @param <T>
     * @return
     */
    public static <T> T getOrElse(T v, T elseV) {
        if (v instanceof String) {
            if (v == null || ((String) v).trim().length() == 0) return elseV;
        } else {
            if (v == null) return elseV;
        }
        return v;
    }

    /**
     * 将以--name=value的命令行形式的参数解析为Map结构
     *
     * @param args
     * @return 如果args为空或null, 则返回一个空的Map;args中非--name=value形式内容则被忽略
     */
    public static Map<String, String> parseCmdArgs(String[] args) {
        Map<String, String> map = new HashMap<String, String>();
        if (args == null) return map;
        for (int i = 0; i < args.length; i++) {
            if(args[i]==null) continue;
            String arg = args[i].trim();
            if (!arg.startsWith("--")) continue;
            Integer idx = arg.indexOf("=");
            if (idx <= 0) continue;
            String key = arg.substring(2, idx);
            String value = arg.substring(idx + 1);
            map.put(key, value);
        }
        return map;
    }

    /**
     * 加载指定的properties文件中的设置项后,再合并args中的设置项,并已args中的设置项目优先
     * @param loader
     * @param propFile
     * @param args
     * @return
     */
    public static Map<String,String> loadPropAndCmdArgs(ClassLoader loader, String propFile,String[] args){
        Map<String, String> argMap = new HashMap<String, String>();
        Properties prop = new Properties();
        try {
            prop.load(loader.getResourceAsStream(propFile));
            for (Map.Entry<Object, Object> entry : prop.entrySet()) {
                argMap.put(entry.getKey().toString(), entry.getValue().toString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Map<String, String> cmdMap = Utils.parseCmdArgs(args);
        argMap.putAll(cmdMap);

        return argMap;
    }



}
