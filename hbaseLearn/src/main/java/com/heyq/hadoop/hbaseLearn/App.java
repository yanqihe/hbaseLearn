package com.heyq.hadoop.hbaseLearn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.heyq.hadoop.hbaseLearn.domain.Student;

public class App {
	// 获取hbase连接
	static Configuration conf = null;
	static Connection conn = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "jikehadoop01,jikehadoop02,jikehadoop03");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		if (conn.isAborted() || conn.isClosed()) {
			System.out.println("获取hbase连接失败");
			return;
		}

		TableName studentTableName = TableName.valueOf("heyanqi:student");

		// 创建表：可重复执行
		List<String> columnFamilyNameList = Arrays.asList("info", "score");
		createTable(studentTableName, columnFamilyNameList);
		System.out.println();

		// 把Tom、Jerry、Jack、Rose和自已的学生信息和成绩保存到hbase的表中
		List<Student> studentList = loadStudentList();
		putStudentList(studentTableName, studentList);

		// 查看所有学生和成绩
		System.out.println("查看heyanqi:student表数据");
		scanFullTable(studentTableName);
		System.out.println("===========================================");
		System.out.println();

		System.out.println("删除‘何衍其’的info列簇class列标识的值");
		deleteData(studentTableName, "何衍其", "info", "class");
		getResult(studentTableName, Arrays.asList("何衍其"));
		System.out.println("===========================================");
		System.out.println();

		System.out.println("删除‘何衍其’的score列簇");
		deleteData(studentTableName, "何衍其", "score");
		getResult(studentTableName, Arrays.asList("何衍其"));
		System.out.println("===========================================");
		System.out.println();

		System.out.println("删除rowKey:‘何衍其’");
		deleteData(studentTableName, "何衍其");
		getResult(studentTableName, Arrays.asList("何衍其"));
		System.out.println("===========================================");
		System.out.println();

		System.out.println("查询1班且understanding成绩大于等于80或programming成绩大于等于80的行");
		scanTableByFilter(studentTableName);

		// 删除表
		deleteTable(studentTableName);
		
		// 关闭连接
		conn.close();
	}

	public static void deleteData(TableName tableName, String rowKey) throws IOException {
		Table table = conn.getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
		table.close();
	}

	/**
	 * 删除某一行的某一个列簇内容
	 */
	public static void deleteData(TableName tableName, String rowKey, String columnFamily) throws IOException {
		Table table = conn.getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addFamily(Bytes.toBytes(columnFamily));
		table.delete(delete);
		table.close();
	}

	/**
	 * 删除某一行某个列簇某列的值
	 */
	public static void deleteData(TableName tableName, String rowKey, String columnFamily, String columnName)
			throws IOException {
		Table table = conn.getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
		table.delete(delete);
		table.close();
	}

	/**
	 * 查询指定的rowKey
	 */
	public static void getResult(TableName tableName, List<String> rowKeyList) throws IOException {
		Table table = conn.getTable(tableName);
		List<Get> getList = new ArrayList<>();

		for (String rowKey : rowKeyList) {
			Get get = new Get(Bytes.toBytes(rowKey));
			getList.add(get);
		}

		Result[] resultList = table.get(getList);
		for (Result result : resultList) {
			String rowData = extractRowData(result);
			System.out.println(rowData);
		}

		table.close();
	}

	/**
	 * 查询指定表的所有数据
	 */
	public static void scanFullTable(TableName tableName) throws IOException {
		Table table = conn.getTable(tableName);
		
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);

		for (Result result : resultScanner) {
			String rowData = extractRowData(result);
			System.out.println(rowData);
		}
	}

	/**
	 * 查询1班且understanding成绩大于等于80或programming成绩大于等于80的行
	 */
	public static void scanTableByFilter(TableName tableName) throws IOException {
		Table table = conn.getTable(tableName);
		Scan scan = new Scan();


		// 过滤是1班的
		FilterList andList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("class"),
				CompareOperator.EQUAL, Bytes.toBytes("1"));
		andList.addFilter(filter1);

		// 且understanding成绩大于等于80或programming成绩大于等于80
		FilterList orList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("score"),
				Bytes.toBytes("understanding"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("80"));
		SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes.toBytes("score"),
				Bytes.toBytes("programming"), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("80"));

		orList.addFilter(filter2);
		orList.addFilter(filter3);
		andList.addFilter(orList);

		scan.setFilter(andList);

		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			String rowData = extractRowData(result);
			System.out.println(rowData);
		}
	}

	public static void putStudentList(TableName tableName, List<Student> studentList) throws IOException {
		if (studentList == null || studentList.size() == 0) {
			return;
		}

		List<Put> puts = new ArrayList<Put>();
		Table table = conn.getTable(tableName);

		for (Student student : studentList) {
			String rowKey = student.getStudentName();
			Put put = new Put(Bytes.toBytes(rowKey));

			String studentInfoColumnFamilyName = "info";
			put.addColumn(Bytes.toBytes(studentInfoColumnFamilyName), Bytes.toBytes("student_id"),
					Bytes.toBytes(student.getInfo().getStudentId()));
			put.addColumn(Bytes.toBytes(studentInfoColumnFamilyName), Bytes.toBytes("class"),
					Bytes.toBytes(student.getInfo().getClazz()));

			String scoreColumnFamilyName = "score";
			put.addColumn(Bytes.toBytes(scoreColumnFamilyName), Bytes.toBytes("understanding"),
					Bytes.toBytes(String.valueOf(student.getScore().getUnderstanding())));
			put.addColumn(Bytes.toBytes(scoreColumnFamilyName), Bytes.toBytes("programming"),
					Bytes.toBytes(String.valueOf(student.getScore().getProgramming())));

			puts.add(put);
		}
		table.put(puts);
		table.close();
	}

	/**
	 * 在创建表之前处理逻辑 1，tableName已存在，则退出； 2，如果tableName对应的namespace没有创建，则先创建
	 */
	public static void createTable(TableName tableName, List<String> columnFamilyNameList) throws IOException {
		if (tableName == null || columnFamilyNameList == null || columnFamilyNameList.size() == 0) {
			return;
		}

		String nameAsString = tableName.getNameAsString();

		Admin admin = conn.getAdmin();
		if (admin.tableExists(tableName)) {
			System.out.println(nameAsString + "  表已存在");
			return;
		}

		final int namespaceDelimIndex = nameAsString.indexOf(TableName.NAMESPACE_DELIM);
		if (namespaceDelimIndex > 0) {
			String namespaceNameStr = nameAsString.substring(0, namespaceDelimIndex);
			if (!namespaceExists(namespaceNameStr)) {
				createNamespace(namespaceNameStr);
			}
		}

		// 表描述器构造器
		TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);

		// 添加列族
		for (String columnFamilyName : columnFamilyNameList) {
			tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamilyName)).build());
		}

		// 创建表
		admin.createTable(tdb.build());
	}
	
	public static void deleteTable(TableName tableName) throws IOException {
		if (tableName == null) {
			return;
		}

		Admin admin = conn.getAdmin();
		if (!admin.tableExists(tableName)) {
			System.out.println(tableName.getNameAsString() + "  表不存在");
			return;
		}
		
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}

	private static boolean namespaceExists(String namespaceNameStr) throws IOException {
		Admin admin = conn.getAdmin();

		List<NamespaceDescriptor> namespaceDescriptorList = Arrays.asList(admin.listNamespaceDescriptors());
		List<String> namespaceList = new ArrayList<>(); 
		for (NamespaceDescriptor namespaceDescr : namespaceDescriptorList) {
			namespaceList.add(namespaceDescr.getName());
		}

		return namespaceList.contains(namespaceNameStr);
	}

	private static NamespaceDescriptor createNamespace(String namespaceNameStr) throws IOException {
		final NamespaceDescriptor HEYANQI_NAMESPACE = NamespaceDescriptor.create(namespaceNameStr).build();

		Admin admin = conn.getAdmin();
		admin.createNamespace(HEYANQI_NAMESPACE);

		return HEYANQI_NAMESPACE;
	}

	private static String extractRowData(Result result) {
		StringBuffer rowData = new StringBuffer();

		String rowKey = Bytes.toString(result.getRow());
		rowData.append("name:" + rowKey + ", ");

		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			String columnKey = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
					+ "::"
					+ Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
			String cellValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
			rowData.append(columnKey + ": " + cellValue + ", ");
		}

		return rowData.substring(0, rowData.length() - 2);
	}

	private static List<Student> loadStudentList() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.YEAR, -1);

		List<Student> studentList = new ArrayList<>();
		studentList.add(new Student("Tom", new Student.Info("20210000000001", "1"), new Student.Score(75, 82)));
		studentList.add(new Student("Jerry", new Student.Info("20210000000002", "1"), new Student.Score(85, 67)));
		studentList.add(new Student("Jack", new Student.Info("20210000000003", "2"), new Student.Score(80, 80)));
		studentList.add(new Student("Rose", new Student.Info("20210000000004", "2"), new Student.Score(60, 61)));
		studentList.add(new Student("何衍其", new Student.Info("G20210735010246", "3"), new Student.Score(95, 96)));
		return studentList;
	}
}
