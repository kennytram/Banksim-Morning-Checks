from Banksim import *
from FileWriter import *
import os
import argparse
from datetime import datetime
import dotenv
import holidays
import smtplib
from email.message import EmailMessage

us_holidays = holidays.US()
REPORT_DIR = "/home/teamsupport2/logs"
TEAM_DIR = "/home/teamsupport2"
my_email = EMAIL_FROM
password = APP_EMAIL_PASSWORD

message = EmailMessage()
# SMTP Server and port no for GMAIL.com
gmail_server = "smtp.gmail.com"
gmail_port = 587

# Starting connection
my_server = smtplib.SMTP(gmail_server, gmail_port)
# my_server.ehlo()
my_server.starttls()
# my_server.ehlo()

# Login with your email and password
my_server.login(my_email, password)


# Brandon
def holiday_check(business_date):
    if isinstance(business_date, str):
        business_date = datetime.strptime(business_date, "%Y%m%d").date()
    if business_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        print(f"{business_date} is a weekend.")
        return True  # Return to indicate it's a weekend
    elif business_date in us_holidays:
        print(f"{business_date} is a holiday: {us_holidays[business_date]}")
        return True  # Return to indicate it's a holiday
    else:
        return False  # Return to indicate it's not a holiday


def get_prev_date(date_str: str) -> str:
    date = datetime.strptime(date_str, "%Y%m%d").date()
    temp = date - timedelta(days=1)
    if date.weekday() == 0:
        temp = date - timedelta(days=3)
    # while holiday_check(date):
    #     temp = temp - timedelta(days=1)
    prev_date = temp.strftime("%Y%m%d")
    return prev_date


dotenv.load_dotenv()
base_dir = "/home/teamsupport2/blobmount"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "business_date",
        type=str,
        help="Business date in YYYYMMDD format",
    )
    args = parser.parse_args()
    # if not holiday_check(args.business_date):
    banksim = Banksim(base_dir, args.business_date)
    business_date = datetime.strptime(args.business_date, "%Y%m%d").date()
    banksim.run()
    banksim.alert_check()
    # banksim.db_manager.create_alert_table()
    # banksim.db_manager.insert_default_alert_rules()
    # banksim.db_manager.create_morning_check_table()
    # fw = FileChecker(base_dir)
    # print(fw.count_csv_rows_matching_files("/home/teamsupport2/blobmount/tba/data/input/ADCC12_repotrades_20240618_1.csv"))
    greet_msg = f'<p style="color:black;">Hello,</p><p style="color:black;">Please find below the Morning Checks report for the day {business_date}.</p>'
    data = banksim.make_health_check_report()
    html_email_data = banksim.make_email_data()

    # Apply bold formatting to system headers
    html_email_data = html_email_data.replace(
        "Trade Booking System (TBA):", "<strong>Trade Booking System (TBA):</strong>"
    )
    html_email_data = html_email_data.replace(
        "Position Management System (PMA):",
        "<strong>Position Management System (PMA):</strong>",
    )
    html_email_data = html_email_data.replace(
        "Credit Risk System (CRS):", "<strong>Credit Risk System (CRS):</strong>"
    )

    # Replace 'GREEN', 'AMBER', 'RED' with appropriate HTML for coloring
    html_email_data = html_email_data.replace(
        "GREEN", '<span style="color:green;"><strong>GREEN</strong></span>'
    )
    html_email_data = html_email_data.replace(
        "AMBER", '<span style="color:orange;"><strong>AMBER</strong></span>'
    )
    html_email_data = html_email_data.replace(
        "RED", '<span style="color:red;"><strong>RED</strong></span>'
    )

    # Add HTML line breaks for proper formatting
    html_email_data = html_email_data.replace("\n", "<br>")

    # Wrap the main content in a black-colored <div> to ensure all non-status text is black
    combined_msg = (
        greet_msg
        + f'<div style="color:black;">{html_email_data}</div>'
        + "<br>"
        + '<p style="color:black;">Regards,<br>TeamSupport2</p>'
    )
    # message.attach(MIMEText(text_content))
    message.set_content(combined_msg, subtype="html")
    message["Subject"] = f"Morning Checks Report for the day {business_date}"
    message["From"] = my_email
    message["To"] = (
        my_email,
        "kennyv.tram@gmail.com",
        "artionkaraj@gmail.com",
        "brandon.wonsung.lee@gmail.com",
    )
    # my_server.sendmail(
    #     from_addr= my_email,
    #     to_addrs=my_email,
    #     msg=message.as_string()
    # )
    my_server.send_message(message)
    file_writer = FileWriter(data)
    file_writer.write(f"morning_report_{args.business_date}.log", REPORT_DIR)
    file_writer.write(f"latest_morning_check_report.log", TEAM_DIR)
    print(data)

    banksim.db_manager.session.close()
