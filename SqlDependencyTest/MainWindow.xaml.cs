/*
 * 使用SqlDependency实现程序对于数据库中表数据变化的监视
 * 
 * 数据库相关的设置
 *  1、设置某个数据库代理的回滚 
 *      ALTER DATABASE [test] SET NEW_BROKER WITH ROLLBACK IMMEDIATE; 
 *  2、设置某个数据库的代理 
 *      ALTER DATABASE [test] SET ENABLE_BROKER; 
 *  3、查询某个数据库是否已经启动了代理 
 *      SELECT name,is_broker_enabled FROM sys.databases WHERE name = 'yaozheng'
 *      is_broker_enabled 为0表示未启动代理 1表示已启动代理  
 * 
 * 注意事项：
 * 1、应用程序开始或者结束时，必须相应的开始或者停止对SQL Server的监控。 
 * 2、只有SQL语句中需要查询的字段才会被监控，没有查询的数据发生变化时，并不会触发dependency_OnChange事件。
 * 3、查询语句只能是简单查询语句,不能带top，不能使用*，不能使用函数包括聚合函数，包括where子查询
 * 4、不能使用外连接、自连接、不能使用临时表、不能用变量、不能用试图、不能跨表、表名前必须加类型dbo的前缀
 * 5、待查询的字段的数据也不能太复杂。测试时，有个字段保存Json格式的数据。如果将这个字段也写入到SQL语句中，则不会被监控到。
 * 6、OnChange只能提供一次通知，如果需要重新发起，需要重新添加事件
 */
using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Windows;

namespace SqlDependencyTest
{
    /// <summary>
    /// MainWindow.xaml 的交互逻辑
    /// </summary>
    public partial class MainWindow : Window
    {
        /// <summary>
        /// 数据库连接信息
        /// </summary>
        private static string _connStr = ConfigurationManager.ConnectionStrings["ConStr"].ConnectionString.ToString();

        /// <summary>
        /// 初始化
        /// </summary>
        public MainWindow()
        {
            InitializeComponent();
            // 启动侦听器来接收来自通过连接字符串指定的 SQL Server 实例的依赖项更改通知。
            SqlDependency.Start(_connStr);
            SelectData();
        }

        /// <summary>
        /// 数据库查询操作
        /// </summary>
        private static void SelectData()
        {
            using (SqlConnection connection = new SqlConnection(_connStr))
            {
                //依赖是基于某一张表的,而且查询语句只能是简单查询语句,不能带top或*,同时必须指定所有者,即类似[dbo].[] 
                string cmdText = "SELECT [ID],[Name],[Age] from dbo.Test_Table where [Age] = 1";
                using (SqlCommand command = new SqlCommand(cmdText, connection))
                {
                    command.CommandType = CommandType.Text;
                    connection.Open();
                    SqlDependency dependency = new SqlDependency(command);
                    // 事件注册，这是核心
                    dependency.OnChange += new OnChangeEventHandler(Dependency_OnChange);

                    SqlDataReader sdr = command.ExecuteReader();
                    Console.WriteLine();
                    while (sdr.Read())
                    {
                        Console.WriteLine("Id:{0}\\Name:{1}\\Age:{2}", sdr["ID"].ToString(), sdr["Name"].ToString(), sdr["Age"].ToString());
                    }
                    sdr.Close();
                }
            }
        }

        /// <summary>
        /// 具体事件
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void Dependency_OnChange(object sender, SqlNotificationEventArgs e)
        {
            // 只有数据发生变化时,才重新获取并数据 
            if (e.Type == SqlNotificationType.Change)
            {
                SelectData();
            }
        }

        /// <summary>
        /// 注意资源的释放 关闭监听
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void Window_Closed(object sender, EventArgs e)
        {
            SqlDependency.Stop(_connStr);
        }
    }
}
