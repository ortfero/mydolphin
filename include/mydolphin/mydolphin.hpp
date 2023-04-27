#pragma once


#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <unordered_map>

#ifdef _WIN32
#    include <winsock.h>
#else
#    include <sys/socket.h>
#endif

#include <mysql.h>



namespace mydolphin {


    enum class error {};


    class error_category: public std::error_category {
        
        char const* name() const noexcept override {
            return "mysql";
        }

        std::string message(int code) const override {
            auto text = std::string {"mysql-"};
            text.append(std::to_string(code));
            return text;
        }

    };   // error_category

    inline error_category const mysql_category;

}   // namespace mydolphin



namespace std {



    template<>
    struct is_error_code_enum<mydolphin::error>: true_type {};



}   // namespace std



namespace mydolphin {



    inline std::error_code make_error_code(error e) noexcept {
        return {int(e), mysql_category};
    }



    struct field {
        std::string name;
    };   // field



    class dataset {
    public:

        using size_type = std::size_t;
        struct record {
            explicit record(MYSQL_ROW row) noexcept: row_(row) {}
            explicit operator bool() const noexcept { return row_ != nullptr; }

            char const* operator[](size_type i) const noexcept {
                return row_[i];
            }

        private:
            char** row_ {nullptr};
        };   // record
    
        using value_type = record;
        using fields_type = std::vector<field>;
        using records_type = std::vector<record>;
        using const_iterator = records_type::const_iterator;
    
    private:
    
        MYSQL_RES* result_ {nullptr};
        records_type records_;
        fields_type fields_;
        
    public:

        dataset() noexcept = default;
        dataset(dataset const&) noexcept = delete;
        dataset& operator=(dataset const&) noexcept = delete;
        explicit operator bool() const noexcept { return result_ != nullptr; }
        bool empty() const noexcept { return records_.empty(); }
        size_type size() const noexcept { return records_.size(); }
        fields_type const& fields() const noexcept { return fields_; }
        const_iterator begin() const noexcept { return records_.begin(); }
        const_iterator end() const noexcept { return records_.end(); }


        ~dataset() noexcept {
            if(result_ != nullptr)
                mysql_free_result(result_);
        }


        dataset(dataset&& other) noexcept
            : result_{other.result_},
              fields_{std::move(other.fields_)},
              records_{std::move(other.records_)} {
            other.result_ = nullptr;
        }


        dataset& operator=(dataset&& other) noexcept {
            if(result_ != nullptr)
                mysql_free_result(result_);
            result_ = other.result_;
            other.result_ = nullptr;
            fields_ = std::move(other.fields_);
            records_ = std::move(other.records_);
            return *this;
        }


        explicit dataset(MYSQL_RES* result) noexcept: result_(result) {
            if(result == nullptr)
                return;

            auto const fields_count = mysql_num_fields(result);
            fields_.resize(fields_count);

            auto const* fields = mysql_fetch_fields(result);
            for(auto i = 0u; i != fields_count; ++i) {
                field& f = fields_[i];
                f.name = fields[i].name;
            }

            auto const records_count = mysql_num_rows(result);
            if(records_count == 0u)
                return;
            records_.reserve(records_count);

            for(auto each_row = mysql_fetch_row(result); !!each_row;
                each_row = mysql_fetch_row(result)) {
                records_.push_back(record{each_row});
            }
        }


        bool operator==(dataset const& other) const noexcept {
            
            if(result_ == other.result_)
                return true;

            if(records_.size() != other.records_.size()
               || fields_.size() != other.fields_.size())
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
        
        
        bool operator!=(dataset const& other) const noexcept {
            return !operator==(other);
        }


        template<typename Stream>
        friend Stream& operator<<(Stream& os, dataset const& ds) noexcept {
            
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

    };   // dataset


    struct credentials {
        std::string host;
        int port {0};
        std::string database;
        std::string user;
        std::string password;

        explicit operator bool() const noexcept {
            return !host.empty() && !user.empty() && !password.empty();
        }

        template<typename Stream>
        friend Stream& operator<<(Stream& stream,
                                  credentials const& data) noexcept {
            stream << "{ host: '" << data.host;
            if(data.port != 0)
                stream << ':' << data.port;
            stream << "', database: '" << data.database << "', user:'"
                   << data.user << "' }";
            return stream;
        }
    };   // credentials

    class connection {
    private:
    
        MYSQL* db_ {nullptr};
        bool authorized_ {false};
        credentials credentials_;
    
    public:
        connection() noexcept = default;
        connection(connection const&) = delete;
        connection& operator=(connection const&) = delete;
        
        bool operator!() const noexcept { return !authorized_; }
        explicit operator bool() const noexcept { return authorized_; }
        bool authorized() const { return authorized_; }
        credentials const& credentials() const { return credentials_; }
        
        ~connection() {
            if(db_ != nullptr)
                mysql_close(db_);
        }
        
        
        std::error_code last_error() const {
            return make_error_code(error(mysql_errno(db_)));
        }

        
        std::string last_error_message() const {
            return std::string(mysql_error(db_));
        }



        connection(struct credentials const& cs) noexcept
            : db_{create_connection()}, credentials_{cs} {
        }


        connection(connection&& other) noexcept
            : db_(other.db_),
              authorized_(other.authorized_),
              credentials_(other.credentials_) {
            other.db_ = nullptr;
        }



        connection& operator=(connection&& other) noexcept {
            if(db_ != nullptr)
                mysql_close(db_);
            db_ = other.db_;
            other.db_ = nullptr;
            authorized_ = other.authorized_;
            credentials_ = other.credentials_;
            return *this;
        }



        bool authorize() noexcept {
            if(db_ == nullptr)
                return false;

            if(authorized_)
                return true;

            if(!credentials_)
                return false;

            authorized_ = mysql_real_connect(db_,
                                             credentials_.host.data(),
                                             credentials_.user.data(),
                                             credentials_.password.data(),
                                             credentials_.database.data(),
                                             credentials_.port,
                                             0,
                                             CLIENT_MULTI_STATEMENTS)
                          != nullptr;

            return authorized_;
        }



        bool ping() noexcept {
            
            if(db_ == nullptr || !authorized_)
                return false;

            auto const ping_result = mysql_ping(db_);
            switch(ping_result) {
            case 0:
                return true;
            case CR_SERVER_GONE_ERROR:
                mysql_close(db_);
                db_ = nullptr;
                authorized_ = false;
                db_ = create_connection();
                return authorize();
            }

            return false;
        }



        bool execute(std::string const& statement) noexcept {
            
            if(!run(statement))
                return false;

            while(mysql_next_result(db_) == 0)
                mysql_free_result(mysql_store_result(db_));

            return true;
        }


        bool query(std::string const& statement, dataset& ds) noexcept {
            
            if(!run(statement))
                return false;

            ds = dataset{mysql_store_result(db_)};

            while(mysql_next_result(db_) == 0)
                mysql_free_result(mysql_store_result(db_));

            return true;
        }


    private:

        static MYSQL* create_connection() {
            auto* db = mysql_init(nullptr);
            bool const reconnect = true;
            mysql_options(db, MYSQL_OPT_RECONNECT, &reconnect);
            return db;
        }


        bool run(std::string const& statement) noexcept {
            
            if(db_ == nullptr || !authorized_)
                return false;

            int query_result = mysql_real_query(db_,
                                                statement.data(),
                                                unsigned(statement.size()));
            if(query_result == CR_SERVER_GONE_ERROR) {
                if(!ping())
                    return false;
                query_result = mysql_real_query(db_,
                                                statement.data(),
                                                unsigned(statement.size()));
            }

            return (query_result == 0);
        }

    };   // connection


    namespace detail {


        template<typename D> constexpr
        std::tuple<std::chrono::year_month_day, std::chrono::hh_mm_ss<D>>
        to_ymd_hms(std::chrono::system_clock::time_point const& tp) {
            namespace chr = std::chrono;
            auto const dp = chr::time_point_cast<chr::days>(tp);
            auto const ymd = chr::year_month_day{dp};
            auto const tod = chr::hh_mm_ss<D>{chr::duration_cast<D>(tp - dp)};
            return {ymd, tod};
        }


        inline void constexpr
        format_year(int year, char*& p) noexcept {
            int digit, rest;
            if(year > 9999) {
                digit = year / 10000;
                rest = year % 10000;
                *p++ = char('0' + digit);
            } else {
                rest = year;
            }
            digit = rest / 1000;
            rest = rest % 1000;
            *p++ = char('0' + digit);
            digit = rest / 100;
            rest = rest % 100;
            *p++ = char('0' + digit);
            digit = rest / 10;
            rest = rest % 10;
            *p++ = char('0' + digit);
            *p++ = char('0' + rest);
        }


        inline void constexpr
        format_00(unsigned n, char*& p) noexcept {
            if(n < 10) {
                *p++ = '0';
                *p++ = char('0' + n);
            } else {
                auto digit = n / 10;
                auto rest = n % 10;
                *p++ = char('0' + digit);
                *p++ = char('0' + rest);
            }
        }


        inline void constexpr
        format_000(unsigned n, char*& p) noexcept {
            unsigned digit = n / 100;
            unsigned rest = n % 100;
            *p++ = char('0' + digit);
            digit = rest / 10;
            rest = rest % 10;
            *p++ = char('0' + digit);
            *p++ = char('0' + rest);
        }


        inline void constexpr
        format_000000(std::uint32_t n, char*& p) noexcept {
            unsigned digit = n / 100000;
            std::uint32_t rest = n % 100000;
            *p++ = char('0' + digit);
            digit = rest / 10000;
            rest = rest % 10000;
            *p++ = char('0' + digit);
            digit = rest / 1000;
            rest = rest % 1000;
            *p++ = char('0' + digit);
            digit = rest / 100;
            rest = rest % 100;
            *p++ = char('0' + digit);
            digit = rest / 10;
            rest = rest % 10;
            *p++ = char('0' + digit);
            *p++ = char('0' + rest);
        }


        inline void constexpr
        append_date(std::chrono::year_month_day ymd, char*& p) {
            format_year(int(ymd.year()), p);
            *p++ = '-';
            format_00(unsigned(ymd.month()), p);
            *p++ = '-';
            format_00(unsigned(ymd.day()), p);
        }


        template<typename D> void constexpr
        append_date_time(std::chrono::year_month_day ymd,
                              std::chrono::hh_mm_ss<D> hms,
                              char*& p) {
            append_date(ymd, p);
            *p++ = ' ';
            format_00(unsigned(hms.hours().count()), p);
            *p++ = ':';
            format_00(unsigned(hms.minutes().count()), p);
            *p++ = ':';
            format_00(unsigned(hms.seconds().count()), p);
        }


        inline void constexpr
        append_date_time_ms(std::chrono::year_month_day ymd,
                            std::chrono::hh_mm_ss<std::chrono::milliseconds> hms,
                            char*& p) {
            append_date_time(ymd, hms, p);
            *p++ = '.';
            format_000(unsigned(hms.subseconds().count()), p);
        }

    } // namespace detail


    inline std::string constexpr
    format_date(std::chrono::system_clock::time_point tp) {
        namespace chr = std::chrono;
        auto const [ymd, _] = detail::to_ymd_hms<chr::seconds>(tp);
        auto formatted = std::string{};
        if(int(ymd.year()) < 10000) {
            formatted.resize(10);
        } else {
            formatted.resize(11);
        }
        auto* p = formatted.data();
        detail::append_date(ymd, p);
        return formatted;
    }


    inline std::string constexpr
    format_date_time(std::chrono::system_clock::time_point tp) {
        namespace chr = std::chrono;
        auto const [ymd, hms] = detail::to_ymd_hms<chr::seconds>(tp);
        auto formatted = std::string{};
        if(int(ymd.year()) < 10000) {
            formatted.resize(19);
        } else {
            formatted.resize(20);
        }
        auto* p = formatted.data();
        detail::append_date_time(ymd, hms, p);
        return formatted;
    }


    inline std::string constexpr
    format_date_time_ms(std::chrono::system_clock::time_point tp) {
        namespace chr = std::chrono;
        auto const [ymd, hms] = detail::to_ymd_hms<chr::milliseconds>(tp);
        auto formatted = std::string{};
        if(int(ymd.year()) < 10000) {
            formatted.resize(23);
        } else {
            formatted.resize(24);
        }
        auto* p = formatted.data();
        detail::append_date_time(ymd, hms, p);
        *p++ = '.';
        detail::format_000(unsigned(hms.subseconds().count()), p);
        return formatted;
    }


    inline std::string constexpr
    format_date_time_us(std::chrono::system_clock::time_point tp) {
        namespace chr = std::chrono;
        auto const [ymd, hms] = detail::to_ymd_hms<chr::microseconds>(tp);
        auto formatted = std::string{};
        if(int(ymd.year()) < 10000) {
            formatted.resize(26);
        } else {
            formatted.resize(27);
        }
        auto* p = formatted.data();
        detail::append_date_time(ymd, hms, p);
        *p++ = '.';
        detail::format_000000(unsigned(hms.subseconds().count()), p);
        return formatted;
    }


}   // namespace mydolphin
