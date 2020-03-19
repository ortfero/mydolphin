#include <iostream>
#include <mydolphin/mydolphin.hpp>



constexpr char const host[] = "localhost";
constexpr char const user[] = "ortfero";
constexpr char const password[] = "Bycbuybz17";



int fail(mydolphin::connection& dc, char const* message) {
  std:: cout << message << ':' << ' ' << dc.last_error_message() << std::endl;
  return 1;
}



int main() {
  using namespace mydolphin;
  using namespace std;

  auto dc = connection{credentials{host, user, password}};
  if (!dc.authorize())
    return fail(dc, "Unable to authorize");

  if (!dc.execute("create database if not exists mydolphin;"))
    return fail(dc, "Unable to create database");

  if (!dc.execute("use mydolphin;"))
    return fail(dc, "Unable to select database");

  if (!dc.execute("drop table if exists samples;"))
    return fail(dc, "Unable to drop table");

  if (!dc.execute("create table samples (id int primary key, title varchar(255));"))
    return fail(dc, "Unable to create table");

  if (!dc.execute("insert into samples(id, title) values (1, 'one'), (2, null), (3, 'three');"))
    return fail(dc, "Unable to insert values");

  if (dc.execute("insert into samples(id, title) values (1, null);"))
    return fail(dc, "Primary key is invalid");

  dataset ds;
  if (!dc.query("select id, title from samples;", ds))
    return fail(dc, "Unable to get rows");

  if (ds.size() != 3)
    return fail(dc, "Invalid dataset");


  return 0;
}
