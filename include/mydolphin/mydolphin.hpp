#pragma once



#include <cstdint>
#include <cstdlib>
#include <string>
#include <ostream>
#include <thread>
#include <unordered_map>
#include <system_error>
#include <optional>

#ifdef _WIN32
#include <winsock.h>
#else
#include <sys/socket.h>
#endif

#include <mysql/mysql.h>



namespace mydolphin {


  
struct credentials {

  std::string host;
  int port{0};
  std::string database;
  std::string user;
  std::string password;

  credentials() = default;
  credentials(credentials const&) = default;
  credentials& operator = (credentials const&) = default;
  credentials(credentials&&) = default;
  credentials& operator = (credentials&&) = default;

  credentials(std::string host, int port, std::string database, std::string user, std::string password):
    host{host}, port{port}, database{database}, user{user}, password{password}
  { }

  credentials(std::string host, std::string database, std::string user, std::string password):
    host{ host }, database{ database }, user{ user }, password{ password }
  { }

  credentials(std::string host, int port, std::string user, std::string password) :
    host{ host }, port{ port }, user{ user }, password{ password }
  { }

  credentials(std::string host, std::string user, std::string password) :
    host{ host }, user{ user }, password{ password }
  { }

  explicit operator bool () const {
    return !host.empty() && !user.empty() && !password.empty();
  }
  


  template<typename charT, typename traits> friend
  std::basic_ostream<charT, traits>&
    operator << (std::basic_ostream<charT, traits>& os, credentials const& cs) noexcept {
      os << "{host:'" << cs.host;
      if(cs.port != 0)
        os << ':' << cs.port;
      os << "' database:" << cs.database << " user:" << cs.user << '}';
      return os;
    }
}; // credentials
	


enum class error { };
	


struct error_category: std::error_category {
  
  char const* name() const noexcept override {
    return "mysql";
  }
  
  std::string message(int code) const noexcept override {
    return std::to_string(code);
  }
  
}; // error_category

inline error_category const mysql_category;

} // mydolphin



namespace std {
  
  
  
template<>
  struct is_error_code_enum<mydolphin::error>: true_type { };
	
	
	
} // std



namespace mydolphin {
	
	

inline std::error_code make_error_code(error e) noexcept {
  return {int(e), mysql_category};
}
  
  
  
struct field {
  std::string name;
}; // field
  


struct dataset {
  using size_type = size_t;
  
  struct record {

    record() noexcept = default;
    record(record const&) noexcept = default;
    record& operator = (record const&) noexcept = default;
    explicit record(MYSQL_ROW row) noexcept: row_(row) { }
    explicit operator bool () const noexcept { return row_ != nullptr; }
    char const* operator [](size_type i) const noexcept { return row_[i]; }


  private:

    char** row_{nullptr};

  }; // record
  
  using value_type = record;
  using fields_type = std::vector<field>;
  using records_type = std::vector<record>;
  using const_iterator = records_type::const_iterator;
    
  dataset() noexcept = default;
  dataset(dataset const&) noexcept = delete;
  dataset& operator = (dataset const&) noexcept = delete;
  explicit operator bool () const noexcept { return result_ != nullptr; }
  bool operator != (dataset const& other) const noexcept { return !operator == (other); }
  bool empty() const noexcept { return records_.empty(); }
  size_type size() const noexcept { return records_.size(); }
  fields_type const& fields() const noexcept { return fields_; }
  const_iterator begin() const noexcept { return records_.begin(); }
  const_iterator end() const noexcept { return records_.end(); }
  
  
  
  ~dataset() noexcept {
    if(result_ != nullptr)
      mysql_free_result(result_);
  }
  
  
  
  dataset(dataset&& other) noexcept:
    result_(other.result_), fields_(std::move(other.fields_)),
    records_(std::move(other.records_)) {
      other.result_ = nullptr;
  }
  
  
  
  dataset& operator = (dataset&& other) noexcept {
    if(result_ != nullptr)
      mysql_free_result(result_);
    result_ = other.result_; other.result_ = nullptr;
    fields_ = std::move(other.fields_);
    records_ = std::move(other.records_);
    return *this;
  }
  
  
  
  dataset(MYSQL_RES* result) noexcept: result_(result) {
	  
    if(result == nullptr)
      return;
  
    auto const fields_count = mysql_num_fields(result);
    fields_.resize(fields_count);
	
    MYSQL_FIELD* fields = mysql_fetch_fields(result); 
    for(int i = 0; i != fields_count; ++i) {
      field& f = fields_[i];
      f.name = fields[i].name;
    }
	
    auto const records_count = mysql_num_rows(result);
    if(records_count == 0)
      return;
    records_.reserve(records_count);
	
    for(auto each_row = mysql_fetch_row(result);
        !!each_row;
        each_row = mysql_fetch_row(result)) {
      records_.push_back(record{each_row});
    }
  }



  bool operator == (dataset const& other) const noexcept {
    
    if(result_ == other.result_)
      return true;
    
    if(records_.size() != other.records_.size() ||
       fields_.size() != other.fields_.size())
      return false;
    
    if(result_ == nullptr || other.result_ == nullptr)
      return false;
    
    auto const records_count = other.records_.size();
    auto const fields_count = fields_.size();
    
    for(records_type::size_type i = 0; i != records_count; ++i)
      for(fields_type::size_type j = 0; j != fields_count; ++j)
        if(strcmp(records_[i][j], other.records_[i][j]) != 0)
          return false;
    
    return true;
  }
  
  
  
  template<typename charT, typename traits> friend
  std::basic_ostream<charT, traits>&
    operator << (std::basic_ostream<charT, traits>& os, dataset const& ds) noexcept {
      
      auto const fields_count = ds.fields_.size();      
      if(fields_count == 0)
        return;
      
      os << ds.fields[0].name();
      
      for(fields_type::size_type i = 1; i != fields_count; ++i)
        os << ',' << ' ' << ds.fields_[i].name();
      
      auto const records_count = ds.records_.size();
      if(records_count == 0)
        return;
      
      for(records_type::size_type i = 0; i != records_count; ++i) {
        
        os << ds.records_[i][0];
        
        for(fields_type::size_type j = 1; j != fields_count; ++j)
          os << ',' << ' ' << ds.records_[i][j];        
      }
      
      return os;
    }
  
private:

  MYSQL_RES* result_{nullptr};
  records_type records_;
  fields_type fields_;
  
}; // dataset



struct connection {

  connection() noexcept: connection(credentials()) { }
  connection(connection const&) noexcept = delete;
  connection& operator = (connection const&) noexcept = delete;
  ~connection() { if(db_ != nullptr) mysql_close(db_); }
  bool operator ! () const noexcept { return !authorized_; }
  explicit operator bool () const noexcept { return authorized_; }
  bool authorized() const { return authorized_; }
  credentials const& credentials() const { return credentials_; }
  std::error_code last_error() const { return make_error_code(error{mysql_errno(db_)}); }
  std::string last_error_message() const { return std::string{mysql_error(db_)}; }
  
  
  
  connection(struct credentials const& cs) noexcept
    : db_(mysql_init(nullptr)), credentials_(cs) {        
    setup();    
  }
  
  
  
  connection(connection&& other) noexcept
    : db_(other.db_), authorized_(other.authorized_),
	    credentials_(other.credentials_) {

    other.db_ = nullptr;  

  }
  
  
  
  connection& operator = (connection&& other) noexcept {
    if(db_ != nullptr)
      mysql_close(db_);
    db_ = other.db_; other.db_ = nullptr;
    authorized_ = other.authorized_;
    credentials_ = other.credentials_;
    return *this;
  }
  
  
  
  bool authorize() noexcept {

    if(db_ == nullptr)
      return false;
    
    if(authorized_)
      return true;
        
    authorized_ = mysql_real_connect(db_, credentials_.host.data(), credentials_.user.data(),
                                  credentials_.password.data(), credentials_.database.data(),
                                  credentials_.port, 0, 0) != nullptr;
                                      
    return authorized_;
  }
  
  
  
  bool ping() noexcept {
    
    if(db_ == nullptr || !authorized_)
      return false;
        
    int const ping_result = mysql_ping(db_);
    switch(ping_result) {
      case 0:
        return true;
      case CR_SERVER_GONE_ERROR:
        mysql_close(db_); db_ = nullptr; authorized_ = false;
        db_ = mysql_init(nullptr);
        setup();
        return authorize();    
    }
    
    return false;
  }
  
  
  
  bool execute(std::string const& s) noexcept {
    return execute(s.data(), unsigned(s.size()));
  }
  
  
  
  bool execute(char const* s) noexcept {
    return execute(s, unsigned(strlen(s)));
  }
  
  
  
  bool execute(char const* query, unsigned length) noexcept {
    
    if(!run(query, length))
      return false;
    
    cleanup();
    
    return true;    
  }
  

  
  bool query(std::string const& s, dataset& ds) noexcept {
    return query(s.data(), unsigned(s.size()), ds);
  }
  

  
  bool query(char const* s, dataset& ds) noexcept {
    return query(s, unsigned(strlen(s)), ds);
  }
  
  

  bool query(char const* s, unsigned length, dataset& ds) noexcept {
    
    if(!run(s, length))
      return false;
    
    ds = dataset(mysql_store_result(db_));
    
    cleanup();
    
    return true; 
  }
	
    
private:


  MYSQL* db_{nullptr};
  bool authorized_{false};
  struct credentials credentials_;
  


  void setup() {
    if(db_ == nullptr)
      return;
    bool const reconnect = true;
    mysql_options(db_, MYSQL_OPT_RECONNECT, &reconnect);	
  }
  

  
  void cleanup() {
    while(mysql_next_result(db_) == 0)
      mysql_free_result(mysql_store_result(db_));    
  }
  


  bool run(char const* s, unsigned length) noexcept {
    
    if(db_ == nullptr)
      return false;
    
    int query_result = mysql_real_query(db_, s, length);
    if(query_result == CR_SERVER_GONE_ERROR) {
      if(!ping())
        return false;
      query_result = mysql_real_query(db_, s, length);
    }
    
    return (query_result == 0);
  }
  
}; // connection



} // mydolphin
