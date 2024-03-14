CREATE OR REPLACE PROCEDURE test_sp()
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  AS
  $$ 
  var cmd = "SELECT my_external_function('Testing') AS return_val";
  var stmt = snowflake.createStatement({sqlText: cmd});
  var result = stmt.execute();
  result.next();
  return result.getColumnValue('return_val');
  $$;
