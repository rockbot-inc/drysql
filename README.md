# drysql
Drysql is simple iimplementation that reduces required boilerplate around maintianing go sql connections


AT the most basic level, you can pass in a pointer to a sql.DB connection to the method GetDrySqlImplementation.  IT will return and instance of drysql that you can start writing queries for.


