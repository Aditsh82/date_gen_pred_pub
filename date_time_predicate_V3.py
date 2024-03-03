from datetime import datetime, timedelta

def generate_date_range(start_date_str, end_date_str, opt="month"):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d %H:%M:%S")
    
    current_date = start_date
    
    while current_date <= end_date:
        if opt == "year":
            yield f"(year='{current_date.year}')"
            next_year = current_date.year + 1
            try:
                current_date = current_date.replace(year=next_year, month=2, day=29)
                current_date = current_date.replace(day=current_date.day - 1) if current_date.day > 1 else current_date
            except ValueError:
                current_date = current_date.replace(year=next_year, month=3, day=1)

        if opt == "month":
            yield f"(year='{current_date.year}', month='{current_date.month}')"
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

        if opt == "day":
            yield f"(year='{current_date.year}', month='{current_date.month}', day='{current_date.day}')"
            current_date += timedelta(days=1)

        if opt == "hour":
            yield f"(year='{current_date.year}', month='{current_date.month}', day='{current_date.day}', hour='{current_date.hour}')"
            current_date += timedelta(hours=1)

# Example call
kk = list(generate_date_range("2020-01-01 00:00:00", "2021-01-01 00:00:00"))
print(kk)
