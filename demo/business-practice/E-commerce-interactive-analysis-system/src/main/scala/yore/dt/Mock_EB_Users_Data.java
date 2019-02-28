package yore.dt;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Random;

/**
 * 电商数据自动生成代码，数据格式如下：
 *
 * 用户信息文件：
 *  用户数据：   {"userID":0,"name":"spark0","registeredTime":"2016-10-11 18:06:25"}
 *
 *  用户访问记录文件：   {"logID":"1","userID":8,"time":"2016-10-09 9:42:45","typed":0,"consumed":404.43}
 *
 * Created by yore on 2019/2/27 14:22
 */
public class Mock_EB_Users_Data {

    public static void main(String[] args) {
        /**
         * 通过传递进来的参数生成指定大小规模的数据
         */
        long numberItems = 1000;
        String dataPath = "demo/business-practice/E-commerce-interactive-analysis-system/src/main/resources/data/";

        if(args.length > 1){
            numberItems = Integer.valueOf(args[0]);
            dataPath = args[1];
        }

        System.out.println("User log number is : " + numberItems);
        mockUserData(numberItems, dataPath);
        mockLogData(numberItems, dataPath);

    }


    /**
     * 生成用户访问日志数据josn文件
     * @param numberItems
     * @param dataPath
     */
    private static void mockLogData(long numberItems, String dataPath) {
        StringBuffer mock_Log_Buffer = new StringBuffer("");
        Random random = new Random();

        for(int i=0; i<numberItems; i++){
            for(int j=0; j<numberItems;j++){
                String initData = "2016-10-";
                String randomData = String.format(
                        "%s%02d%s%02d%s%02d%s%02d",
                        initData,
                        random.nextInt(31),
                        " ",
                        random.nextInt(24),
                        ":",
                        random.nextInt(60),
                        ":",
                        random.nextInt(60)
                );
                String result = "{\"logID\":\"#1\",\"userID\":#2,\"time\":\"#3\",\"typed\":#4,\"consumed\":#5}"
                        .replace("#1", String.format("%02d", j))
                        .replace("#2", String.valueOf(i))
                        .replace("#3", randomData)
                        .replace("#4", String.format("%01d", random.nextInt(2)))
                        .replace("#5", String.format("%.2f", random.nextDouble() * 1000));
                mock_Log_Buffer.append(result).append("\n");
            }
        }
        System.out.println(mock_Log_Buffer);
        PrintWriter printWriter = null;
        try {
            // 保存JSON文件
            printWriter = new PrintWriter(new OutputStreamWriter(
                    new FileOutputStream(dataPath + "Mock_EB_Log_Data.json")
            ));
            printWriter.write(mock_Log_Buffer.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            printWriter.close();
        }

    }

    /**
     * 生成用户信息json数据文件
     * @param numberItems
     * @param dataPath
     */
    private static void mockUserData(long numberItems, String dataPath) {
        StringBuffer mock_User_Buffer = new StringBuffer("");
        Random random = new Random();

        for(int i=0; i<numberItems; i++){
            String initData = "2016-10-";
            String randomData = String.format(
                    "%s%02d%s%02d%s%02d%s%02d",
                    initData,
                    random.nextInt(31),
                    " ",
                    random.nextInt(24),
                    ":",
                    random.nextInt(60),
                    ":",
                    random.nextInt(60)
            );
            String result = "{\"userID\":#1,\"name\":\"spark#2\",\"registeredTime\":\"#3\"}"
                    .replace("#1", String.valueOf(i))
                    .replace("#2", String.valueOf(i))
                    .replace("#3", randomData);
            mock_User_Buffer.append(result).append("\n");

        }
        System.out.println(mock_User_Buffer);
        PrintWriter printWriter = null;
        try {
            // 保存JSON文件
            printWriter = new PrintWriter(new OutputStreamWriter(
                    new FileOutputStream(dataPath + "Mock_EB_Users_Data.json")
            ));
            printWriter.write(mock_User_Buffer.toString());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            printWriter.close();
        }


    }


}
