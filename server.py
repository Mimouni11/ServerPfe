from flask import Flask, request, jsonify, render_template, redirect, session, url_for
import mysql.connector
from flask_session import Session

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


app = Flask(__name__)
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)
# Configuration for MySQL database
db_config = {
    'host': 'localhost',
    'user': 'me',
    'password': '0000',
    'database': 'pfe'  # Your database name
}


EMAIL_HOST = 'smtp.gmail.com'
EMAIL_PORT = 587
EMAIL_USER = 'amberelmedina1@gmail.com'
EMAIL_PASSWORD = 'cnlr aouk xqbl wayb'


def send_email(receiver_email, subject, message_body):
    try:
        # Create a secure SSL context
        server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)

        # Construct the email message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = receiver_email
        msg['Subject'] = subject
        msg.attach(MIMEText(message_body, 'plain'))

        # Send the email
        server.sendmail(EMAIL_USER, receiver_email, msg.as_string())

        # Close the connection
        server.quit()

        return True
    except Exception as e:
        print("Error sending email:", e)
        return False


# Function to authenticate user
# Function to authenticate user and get user role
def authenticate_user(username, password):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Query the database to check if username and password are valid
        query = "SELECT Role FROM users WHERE Username = %s AND Password = %s"
        cursor.execute(query, (username, password))

        # Fetch user role along with authentication status
        user_data = cursor.fetchone()
        if user_data:
            role = user_data[0]
            return True, role
        else:
            return False, None

    except mysql.connector.Error as error:
        print("Error: {}".format(error))
        return False, None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# Route for authentication
@app.route('/authenticate', methods=['POST'])
def authenticate():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        # Call the authenticate_user function
        auth_status, user_role = authenticate_user(username, password)
        if auth_status:
            return jsonify({'status': 'success', 'message': 'Authentication successful', 'role': user_role})
            session['username'] = username  
            session['user_role']=user_role
        else:
            return jsonify({'status': 'failure', 'message': 'Authentication failed'})


@app.route('/profile')
def profile():
    if 'username' in session:
        username = session['username']
        return render_template('profile.html', username=username)
    else:
        return redirect(url_for('login'))






@app.route('/add_user', methods=['POST'])
def add_user():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        role = request.form['role']

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()

            # Insert new user into the database
            query = "INSERT INTO users (Username, Mail, Password, Role) VALUES (%s, %s, %s, %s)"
            cursor.execute(query, (username, email, password, role))
            connection.commit()

            # Send email to the user
            email_subject = "mar7be bik fil pfe mte3i"
            email_message = f"aaslema {username},\n\nYour account has been successfully created.\n\nUsername: {username}\nPassword: {password}"
            if send_email(email, email_subject, email_message):
                return jsonify({'status': 'success', 'message': 'User added successfully and email sent'})
            else:
                return jsonify({'status': 'success', 'message': 'User added successfully, but email sending failed'})
        except mysql.connector.Error as error:
            print("Error: {}".format(error))
            return jsonify({'status': 'failure', 'message': 'Failed to add user'})

        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()

















@app.route('/change_password', methods=['POST'])
def change_password():
    if request.method == 'POST':
        username = request.form['username']
        old_password = request.form['old_password']
        new_password = request.form['new_password']

        # Authenticate the user
        if authenticate_user(username, old_password):
            try:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()

                # Update the password in the database
                query = "UPDATE users SET Password = %s WHERE Username = %s"
                cursor.execute(query, (new_password, username))
                connection.commit()

                return jsonify({'status': 'success', 'message': 'Password changed successfully'})
            except mysql.connector.Error as error:
                print("Error: {}".format(error))
                return jsonify({'status': 'failure', 'message': 'Failed to change password'})

            finally:
                if 'connection' in locals() and connection.is_connected():
                    cursor.close()
                    connection.close()
        else:
            return jsonify({'status': 'failure', 'message': 'Invalid old password'})


if __name__ == '__main__':
    app.run(host='192.168.1.5', port=5001, debug=True)