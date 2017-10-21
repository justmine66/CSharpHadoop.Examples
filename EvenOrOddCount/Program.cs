using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.WebClient.WebHCatClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EvenOrOddCount
{
    class Program
    {
        static void Main(string[] args)
        {
            //建立作业配置    
            var jobConfig = new HadoopJobConfiguration();
            jobConfig.InputPath = "/demo/simple/in";
            jobConfig.OutputFolder = "/demo/simple/out";

            //连接到集群
            var clusterName = new Uri("http://localhost");
            string userName = "hadoop";
            string passWord = null;
            IHadoop myCluster = Hadoop.Connect(clusterName, userName, passWord);

            //执行MapReduce作业
            MapReduceResult result = myCluster.MapReduceJob.Execute<MySimpleMapper, MySimpleReducer>(jobConfig);

            //输出结果到控制台
            int exitCode = result.Info.ExitCode;
            var exitStatus = "Failure";
            if (exitCode==0)
            {
                exitStatus = "Success";
            }
            exitStatus = exitCode + " (" + exitStatus + ")";
            Console.WriteLine();     
            Console.Write("Exit Code = " + exitStatus);
            Console.Read();
        }
    }
}
