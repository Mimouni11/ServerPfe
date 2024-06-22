from flask import Flask, request, jsonify, render_template, redirect, session, url_for
import mysql.connector
from flask_session import Session
from flask_cors import CORS, cross_origin
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date
from datetime import datetime,timezone,timedelta
import logging
import qrcode
import io
import requests
import secrets  # Import the secrets module
import base64
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from collections import defaultdict
import subprocess


app = Flask(__name__)
CORS(app, origins=['http://192.168.1.172:3000'], supports_credentials=True)  # Allow CORS with credentials
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)

db_config = {
    'host': 'localhost',
    'user': 'me',
    'password': '0000',
    'database': 'pfe' 
}


EMAIL_HOST = 'smtp.office365.com'
EMAIL_PORT = 587
EMAIL_USER = 'sawkeji.inc@outlook.com'
EMAIL_PASSWORD = 'xmsxoxhmrietjpwb'


def send_email(receiver_email, subject, message_body, attachment_path=None):
    try:
        server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASSWORD)

        # Create a multipart message and set headers
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = receiver_email
        msg['Subject'] = subject
        msg.attach(MIMEText(message_body, 'plain'))

        if attachment_path:
            # Attach PDF file
            with open(attachment_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {attachment_path}",
            )
            msg.attach(part)

        server.sendmail(EMAIL_USER, receiver_email, msg.as_string())
        server.quit()

        return True
    except Exception as e:
        print("Error sending email:", e)
        return False




import uuid
reset_tokens = {}


@app.route('/reset_password', methods=['POST'])
def reset_password():
    data = request.get_json()
    email = data.get('email')

    # Generate a unique token
    token = str(uuid.uuid4())
    reset_tokens[token] = email

    # Construct the reset link
    reset_link = url_for('reset_password_with_token', token=token, _external=True)

    # Send email (assuming send_email function is defined)
    send_email(email, "Password Reset", f"Click the link to reset your password: {reset_link}")

    return jsonify({'status': 'success', 'message': 'Password reset email sent.'})


from flask import flash
app.secret_key = 'your_secret_key'  # needed for flashing messages
@app.route('/reset/<token>', methods=['GET', 'POST'])
def reset_password_with_token(token):
    if request.method == 'GET':
        return render_template('reset_password.html', token=token)

    if request.method == 'POST':
        new_password = request.form.get('new_password')
        email = reset_tokens.get(token)

        if email:
            try:
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()
                query = "UPDATE users SET password = %s WHERE Mail= %s"
                cursor.execute(query, (new_password, email))
                connection.commit()
                cursor.close()
                connection.close()
                del reset_tokens[token]
                flash("Your password has been changed successfully. You can now log in with your new password.")
            except mysql.connector.Error as err:
                flash(f"Error updating password: {err}")
        else:
            flash("Invalid token")
        return render_template('reset_password_result.html')  # Render a simple template with the flash messages


@app.route('/add_user', methods=['POST'])
@cross_origin(supports_credentials=True)
def add_user():
    if request.method == 'POST':
        data = request.get_json()  # Parse JSON data
        username = data.get('username')
        email = data.get('email')
        role = data.get('role')
        vehicle_type = data.get('vehicleType')   # Get vehicleType parameter, default to None if not provided

        # Generate a random password
        password = secrets.token_urlsafe(8)  # Generate an 8-character random password

        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()

            # Insert new user into the users table
            query_user = "INSERT INTO users (Username, Mail, Password, Role) VALUES (%s, %s, %s, %s)"
            cursor.execute(query_user, (username, email, password, role))
            connection.commit()

            # Get the ID of the newly inserted user
            user_id = cursor.lastrowid

            # Insert user into their corresponding table based on role, including the user_id
            if role == "driver":
                query_driver = "INSERT INTO drivers (id, username, password, type) VALUES (%s, %s, %s, %s)"
                cursor.execute(query_driver, (user_id, username, password, vehicle_type))
            elif role == "mechanic":
                query_mechanic = "INSERT INTO mecano (id, name, password) VALUES (%s, %s, %s)"
                cursor.execute(query_mechanic, (user_id, username, password))
            elif role == "chef":
                query_chef = "INSERT INTO chef (idchef, username, password) VALUES (%s, %s, %s)"
                cursor.execute(query_chef, (user_id, username, password))

            connection.commit()

            # Send email to the user with PDF attachment
            email_subject = "Marhbe bik fil pfe mteei!"
            email_message = f"Hello {username},\n\nYour account has been successfully created.\n\nUsername: {username}\nPassword: {password}\nRole: {role}"
            # Get the current directory
            current_directory = os.getcwd()
            # Construct the path to the PDF file
            pdf_attachment_path = os.path.join(current_directory, "haha.pdf")
            if send_email(email, email_subject, email_message, pdf_attachment_path):
                return jsonify({'status': 'success', 'message': 'User added successfully and email sent'})
            else:
                return jsonify({'status': 'failure', 'message': 'Failed to send email with PDF attachment'})
        except mysql.connector.Error as error:
            print("Error: {}".format(error))
            return jsonify({'status': 'failure', 'message': 'Failed to add user'})
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()



def authenticate_user(username, password):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Query to check username and password
        query = "SELECT Role FROM users WHERE Username = %s AND Password = %s"
        cursor.execute(query, (username, password))

        user_data = cursor.fetchone()
        if user_data:
            role = user_data[0]
            if role == 'driver':
                # Query to get the type for the driver
                type_query = "SELECT Type FROM drivers WHERE Username = %s"
                cursor.execute(type_query, (username,))
                type_data = cursor.fetchone()
                if type_data:
                    user_type = type_data[0]
                else:
                    user_type = None
                return True, role, user_type
            else:
                return True, role, None
        else:
            return False, None, None

    except mysql.connector.Error as error:
        print("Error: {}".format(error))
        return False, None, None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()



def authenticate_admin_user(username, password):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = "SELECT Role FROM users WHERE Username = %s AND Password = %s"
        cursor.execute(query, (username, password))

        user_data = cursor.fetchone()
        if user_data:
            role = user_data[0]
            if role == 'admin' or role == 'chef':  # Check for both admin and chef roles
                return True, role
            else:
                return False, role  # Return the role, which is neither admin nor chef
        else:
            return False, None

    except mysql.connector.Error as error:
        print("Error: {}".format(error))
        return False, None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()







@app.route('/updateStatus', methods=['POST'])
def update_status():
    data = request.get_json()
    username = data.get('username')
    status = data.get('status')
    
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    
    # Check if the user exists
    query_get_id = "SELECT idusers FROM users WHERE Username = %s"
    cursor.execute(query_get_id, (username,))
    user_id = cursor.fetchone()

    if user_id:
        user_id = user_id[0]
        
        # Update the user's status and last_activity_at
        query_update_status = """
            UPDATE users 
            SET status = %s, last_activity_at = NOW() 
            WHERE idusers = %s
        """
        cursor.execute(query_update_status, (status, user_id))
        connection.commit()
        
        cursor.close()
        connection.close()
        
        return jsonify({'message': 'User status and activity updated successfully'}), 200
    else:
        cursor.close()
        connection.close()
        return jsonify({'message': 'User not found'}), 404






@app.route('/free-mechanics', methods=['GET', 'POST'])
@cross_origin(supports_credentials=True)

def get_free_mechanics():
    date = request.json.get('date')

    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Query to find mechanics who either have no tasks or have less than 3 tasks on the specified date
        query = """
 SELECT id, name
FROM mecano
WHERE id NOT IN (
    SELECT id_mecano
    FROM mecano_tasks
    WHERE date= %s
) OR id IN (
    SELECT id_mecano
    FROM mecano_tasks
    WHERE date = %s
    GROUP BY id_mecano
    HAVING COUNT(*) < 3
);




        """
        cursor.execute(query, (date, date))
        mechanics = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]

        return jsonify({'mechanics': mechanics})

    except mysql.connector.Error as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to fetch free mechanics'})


@app.route('/free-drivers', methods=['POST'])  # Only POST method is needed
@cross_origin(supports_credentials=True)
def get_free_drivers():
    data = request.json
    date = data.get('date')
    selected_type = data.get('type')  # Get selected type from request payload
    print("Received date:", date)
    print("Received type:", selected_type)
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        date_form =datetime.strptime(date, '%Y-%m-%d')
        next_day = date_form + timedelta(days=1)
        next_day_str = next_day.strftime('%Y-%m-%d')    
        # Query to find drivers who either have no tasks or have tasks not scheduled for the specified date and type
        query = """
        SELECT id, username
        FROM drivers
        WHERE id NOT IN (
            SELECT id_driver
            FROM driver_tasks
            WHERE date = %s OR date IS NULL
        )
        AND type = %s;  -- Filter by selected type
        """
        cursor.execute(query, (next_day_str, selected_type))  # Pass date and type as a tuple
        print("Executed SQL query:", cursor.statement)
        drivers = [{'id': row[0], 'username': row[1]} for row in cursor.fetchall()]
        
        return jsonify({'drivers': drivers})

    except mysql.connector.Error as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to fetch free drivers'}), 500


@app.route('/insert-tasks', methods=['POST'])
@cross_origin(supports_credentials=True)
def insert_tasks():
    # Extract data from the request body
    data = request.json
    id_mecano = data.get('id_mecano')
    tasks = data.get('tasks')
    date = data.get('date')
    model = data.get('model')  # Extract model from the request
    matricule = data.get('matricule')
    tasktype = data.get('tasktype')  # Ensure tasktype is correctly extracted
    print("Received date:", date)
    print("Received tasktype:", tasktype)  # Debug line to ensure tasktype is received

    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        date_form = datetime.strptime(date, '%Y-%m-%d')
        next_day = date_form + timedelta(days=1)
        next_day_str = next_day.strftime('%Y-%m-%d')
        
        # Insert tasks into the mecano_tasks table
        insert_task_query = """
        INSERT INTO mecano_tasks (id_mecano, tasks, model, matricule, task_type, date)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_task_query, (id_mecano, tasks, model, matricule, tasktype, next_day_str))
        
        # Update the truck status and next maintenance date based on the task type
        if tasktype == 'reparation':
            update_truck_status_query = """
            UPDATE trucks
            SET status = 'en panne'
            WHERE matricule = %s
            """
            cursor.execute(update_truck_status_query, (matricule,))
        elif tasktype == 'maintenance':
            update_truck_status_query = """
            UPDATE trucks
            SET status = 'maintenance', next_maintenance_date = %s
            WHERE matricule = %s
            """
            cursor.execute(update_truck_status_query, (next_day_str, matricule,))
        
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Tasks inserted and truck status updated successfully'})

    except mysql.connector.Error as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to insert tasks and update truck status'}), 500





@app.route('/pending-tasks', methods=['GET'])
@cross_origin(supports_credentials=True)
def fetch_pending_tasks():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        query = "SELECT * FROM mecano_tasks WHERE status = 'pending'"
        cursor.execute(query)
        tasks = cursor.fetchall()
        
        # Transform the result into a list of dictionaries
        tasks_list = [
            {
                'id': task[0],
                'id_mecano': task[1],
                'tasks': task[2],
                'date': task[3],
                'model': task[4],
                'matricule': task[5]
            }
            for task in tasks
        ]
        
        cursor.close()
        connection.close()
        
        return jsonify({'tasks': tasks_list})
    except mysql.connector.Error as error:
        return jsonify({'error': str(error)}), 500




@app.route('/pending-tasks-driver', methods=['GET'])
@cross_origin(supports_credentials=True)
def fetch_pending_tasks_driver():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        query = "SELECT * FROM driver_tasks WHERE status = 'pending'"
        cursor.execute(query)
        tasks = cursor.fetchall()
        
        # Transform the result into a list of dictionaries
        tasks_list = [
            {
                'idtask': task[0],
                'id_driver': task[1],
                'task': task[2],
                'date': task[3],
                'matricule': task[4]
            }
            for task in tasks
        ]
        
        cursor.close()
        connection.close()
        
        return jsonify({'tasks': tasks_list})
    except mysql.connector.Error as error:
        return jsonify({'error': str(error)}), 500



@app.route('/approve-task', methods=['POST'])
@cross_origin(supports_credentials=True)
def approve_task():
    task_id = request.args.get('task_id')  # Get task_id from query parameters

    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        query = "UPDATE mecano_tasks SET status = 'approved' WHERE idmecano_tasks = %s"
        cursor.execute(query, (task_id,))
        connection.commit()
        
        cursor.close()
        connection.close()
        
        # Send notification to the mechanic here (you can reuse your existing function)
        
        return jsonify({'message': 'Task approved successfully'}), 200
    except mysql.connector.Error as error:
        logging.error(f"Error approving task: {error}")
        return jsonify({'error': str(error)}), 500


@app.route('/approve-task-driver', methods=['POST'])
@cross_origin(supports_credentials=True)
def approve_task_driver():
    task_id = request.args.get('task_id')  # Get task_id from query parameters

    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        query = "UPDATE driver_tasks SET status = 'approved' WHERE idtask = %s"
        cursor.execute(query, (task_id,))
        connection.commit()
        
        cursor.close()
        connection.close()
        
        # Send notification to the mechanic here (you can reuse your existing function)
        
        return jsonify({'message': 'Task approved successfully'}), 200
    except mysql.connector.Error as error:
        logging.error(f"Error approving task: {error}")
        return jsonify({'error': str(error)}), 500
















@app.route('/insert-tasks-driver', methods=['POST'])
@cross_origin(supports_credentials=True)
def insert_tasks_driver():
    try:
        # Extract data from the request body
        data = request.json
        id_task = data.get('id_task')
        id_driver = data.get('username')
        task_description = data.get('task')
        date = data.get('date')
        matricule=data.get('matricule')
        print('data',data)
        print(date);
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        date_form =datetime.strptime(date, '%Y-%m-%d')
        next_day = date_form + timedelta(days=1)
        next_day_str = next_day.strftime('%Y-%m-%d')        
        
        # Insert the task into the driver_tasks table
        print(next_day_str);
        query = """
        INSERT INTO driver_tasks (idtask, id_driver, date, task, matricule)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (id_task, id_driver, next_day_str, task_description,matricule))
        connection.commit()

        return jsonify({'message': 'Task inserted successfully'})
    except Exception as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to insert task'}), 500



@app.route('/search-matricule', methods=['POST'])
@cross_origin(supports_credentials=True)
def search_matricule():
    # Get the matricule from the request body
    matricule = request.json.get('matricule')
    
    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        print("matricule",matricule)
        # Query to fetch tasks information based on the provided matricule
        query = """
        SELECT idtask, date, task, matricule
        FROM driver_tasks
        WHERE matricule = %s
        """
        cursor.execute(query, (matricule,))
        print("Executed SQL query:", cursor.statement)

        # Fetch all rows
        tasks_data = cursor.fetchall()

        # Close cursor and connection
        cursor.close()
        connection.close()

        # Check if tasks exist
        if not tasks_data:
            return jsonify({'error': 'Tasks not found'}), 404

        return jsonify({'tasks_data': tasks_data})

    except mysql.connector.Error as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to search tasks'}), 500













@app.route('/authenticate', methods=['POST'])
@cross_origin(supports_credentials=True)
def authenticate():
    if request.method == 'POST':
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')

        # Call the authenticate_user function
        auth_status, user_role, user_type = authenticate_user(username, password)
        if auth_status:
            session['username'] = username  
            session['user_role'] = user_role
            response = {'status': 'success', 'message': 'Succès d"authentification', 'role': user_role}
            if user_role == 'driver':
                response['type'] = user_type  # Add the actual type for drivers
            return jsonify(response)
        else:
            return jsonify({'status': 'failure', 'message': 'Échec d"Authentification'})





@app.route('/admin/authenticate', methods=['POST'])
@cross_origin(supports_credentials=True)
def authenticate_admin():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        # Call the authenticate_admin_user function
        auth_status, user_role = authenticate_admin_user(username, password)
        if auth_status:
            session['username'] = username
            session['user_role'] = user_role
            if user_role == 'admin':
                return jsonify({'status': 'success', 'message': 'Admin authentication successful','username':username, 'role': user_role})
            elif user_role == 'chef':
                return jsonify({'status': 'success', 'message': 'Admin authentication successful','username':username, 'role': user_role})
            else:
                return jsonify({'status': 'failure', 'message': 'Invalid role'})
        else:
            return jsonify({'status': 'failure', 'message': 'Authentication failed'})





@app.route('/addcar', methods=['POST'])
@cross_origin(supports_credentials=True)
def add_car():
    try:
        data = request.json
        matricule = data['matricule']
        vehicle_type = data['vehicleType']

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Insert new vehicle into the trucks table
        query_truck = "INSERT INTO trucks (matricule, type) VALUES (%s, %s)"
        cursor.execute(query_truck, (matricule, vehicle_type))
        connection.commit()

        # Insert vehicle into their corresponding table based on vehicle_type
        if vehicle_type == "pickup":
            query_pickup = "INSERT INTO pickup (matricule) VALUES (%s)"
            cursor.execute(query_pickup, (matricule,))
        elif vehicle_type == "truck":
            query_truck_table = "INSERT INTO truck (matricule) VALUES (%s)"
            cursor.execute(query_truck_table, (matricule,))
        elif vehicle_type == "semi":
            query_semi = "INSERT INTO semi (matricule) VALUES (%s)"
            cursor.execute(query_semi, (matricule,))

        connection.commit()
        return jsonify({"message": "Car added successfully"}), 200

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return jsonify({"error": "Failed to add car"}), 500

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()














@app.route('/tasks', methods=['GET'])
@cross_origin(supports_credentials=True)
def fetch_tasks_for_current_user_and_date():
    try:
        app.logger.info('Request Args: %s', request.args)
        username = request.args.get('username')

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        current_date = date.today().isoformat()

        cursor.execute("SELECT id FROM mecano WHERE name = %s", (username,))
        user_id = cursor.fetchone()

        if not user_id:
            return jsonify({'error': 'User not found'}), 404

        query = """
        SELECT tasks, model, matricule, task_type
        FROM mecano_tasks
        WHERE id_mecano = %s AND date = %s 
        """
        cursor.execute(query, (user_id[0], current_date))
        print("Executed SQL query:", cursor.statement)

        tasks = cursor.fetchall()
        cursor.close()
        connection.close()

        return jsonify({'tasks': [{'task': task[0], 'model': task[1], 'matricule': task[2], 'taskType': task[3]} for task in tasks]})

    except mysql.connector.Error as error:
        return jsonify({'error': str(error)}), 500



@app.route('/update_task_status', methods=['POST'])
@cross_origin(supports_credentials=True)
def update_task_status():
    try:
        username = request.form.get('username')
        task_name = request.form.get('taskName')
        status = "pending_confirmation"

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        cursor.execute("SELECT id FROM mecano WHERE name = %s", (username,))
        user_id = cursor.fetchone()

        if not user_id:
            return jsonify({'error': 'User not found'}), 404

        cursor.execute("UPDATE mecano_tasks SET done = %s WHERE id_mecano = %s AND tasks = %s", (status, user_id[0], task_name))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Task status updated to pending confirmation'}), 200

    except mysql.connector.Error as error:
        return jsonify({'error': str(error)}), 500
    




@app.route('/get_pending_tasks', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_pending_tasks():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        cursor.execute("""
            SELECT mecano.name, mecano_tasks.tasks, mecano_tasks.matricule, mecano_tasks.task_type 
            FROM mecano_tasks 
            JOIN mecano ON mecano.id = mecano_tasks.id_mecano 
            WHERE mecano_tasks.done = 'pending_confirmation'
        """)
        tasks = cursor.fetchall()

        task_list = [{'mechanic': task[0], 'task': task[1], 'matricule': task[2], 'taskType': task[3]} for task in tasks]

        cursor.close()
        connection.close()
        print(task_list)
        return jsonify({'tasks': task_list}), 200

    except mysql.connector.Error as error:
        return jsonify({'error': str(error)}), 500



@app.route('/confirm_task', methods=['POST'])
@cross_origin(supports_credentials=True)
def confirm_task():
    task_name = request.form.get('taskName')
    matricule = request.form.get('matricule')
    task_type = request.form.get('taskType')

    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Update the task status in the database
        query = """
        UPDATE mecano_tasks
        SET done = 'yes'
        WHERE tasks = %s
        """
        cursor.execute(query, (task_name,))
        connection.commit()
        
        # Update the truck status and maintenance/reparation dates based on task type
        if task_type == 'maintenance':
            query2 = """
            UPDATE trucks 
            SET status = 'dispo', last_maintenance_date = CURDATE()
            WHERE matricule = %s
            """
        elif task_type == 'reparation':
            query2 = """
            UPDATE trucks 
            SET status = 'dispo', last_repared_at = CURDATE()
            WHERE matricule = %s
            """
        else:
            query2 = """
            UPDATE trucks 
            SET status = 'dispo'
            WHERE matricule = %s
            """
        
        cursor.execute(query2, (matricule,))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Task confirmed successfully'})

    except Exception as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to confirm task'}), 500



@app.route('/tasks-driver', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_tasks_for_scanned_content():
    try:
        # Get the scanned content and username from the request
        content = request.args.get('content')
        username = request.args.get('username')

        if not username:
            logging.error("Username is missing from the request")
            return jsonify({'error': 'Username is required'}), 400

        if not content:
            logging.error("Content is missing from the request")
            return jsonify({'error': 'Content is required'}), 400

        # Split the content string into individual primary keys
        primary_keys = content.split(',')

        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Get the driver's ID from the drivers table using the username
        cursor.execute("SELECT id FROM drivers WHERE username = %s", (username,))
        driver_id_result = cursor.fetchone()
        
        if not driver_id_result:
            logging.error(f"Driver not found for username: {username}")
            return jsonify({'error': 'Driver not found'}), 404

        driver_id = driver_id_result[0]

        # Initialize an empty list to store tasks
        tasks_list = []

        # Get today's date in the format YYYY-MM-DD
        today_date = datetime.now().strftime('%Y-%m-%d')

        # Search for tasks related to each primary key and the driver's ID
        for key in primary_keys:
            query = """
            SELECT * FROM driver_tasks WHERE idtask = %s AND id_driver = %s AND DATE(date) = %s
            """
            cursor.execute(query, (key, driver_id, today_date))
            tasks = cursor.fetchall()

            # Append tasks to the tasks_list
            for task in tasks:
                tasks_list.append({
                    'task': task[2],  # Assuming task details are in the third column
                    'matricule': task[4]  # Assuming matricule is in the fifth column
                })

        cursor.close()
        connection.close()

        return jsonify(tasks_list)
    except mysql.connector.Error as e:
        logging.error(f"MySQL Error: {e}")
        return jsonify({'error': f'MySQL Error: {str(e)}'}), 500
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({'error': str(e)}), 500




@app.route('/get-matricule', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_matricule():
    try:
        # Get the username from the request
        username = request.args.get('username')

        if not username:
            logging.error("Username is missing from the request")
            return jsonify({'error': 'Username is required'}), 400

        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Get the driver's ID from the drivers table using the username
        cursor.execute("SELECT id FROM drivers WHERE username = %s", (username,))
        driver_id_result = cursor.fetchone()
        
        if not driver_id_result:
            logging.error(f"Driver not found for username: {username}")
            return jsonify({'error': 'Driver not found'}), 404

        driver_id = driver_id_result[0]

        # Get today's date in the format YYYY-MM-DD
        today_date = datetime.now().strftime('%Y-%m-%d')

        # Search for the matricule in the driver tasks table using the driver's ID and today's date
        query = """
        SELECT matricule FROM driver_tasks WHERE id_driver = %s AND DATE(date) = %s
        """
        cursor.execute(query, (driver_id, today_date))
        matricule_result = cursor.fetchone()

        cursor.close()
        connection.close()

        if not matricule_result:
            logging.error(f"No tasks found for driver ID: {driver_id} on date: {today_date}")
            return jsonify({'error': 'No tasks found for today'}), 404

        matricule = matricule_result[0]

        return jsonify({'matricule': matricule})

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return jsonify({'error': 'An error occurred'}), 500






@app.route('/free-trucks', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_free_trucks():
    try:
        # Get the date and type from the request
        date = request.args.get('date')
        truck_type = request.args.get('type')
        print('truck type',truck_type)
        print('date',date)
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        date_form =datetime.strptime(date, '%Y-%m-%d')
        next_day = date_form + timedelta(days=1)
        next_day_str = next_day.strftime('%Y-%m-%d') 
        # Query to find free trucks based on type and date
        query = """
      SELECT matricule, type
FROM trucks
WHERE 
    (matricule NOT IN (
        SELECT matricule
        FROM driver_tasks
        WHERE date = %s
    ) 
    OR 
    matricule IN (
        SELECT matricule
        FROM driver_tasks
        WHERE date != %s
    )) 
    AND 
    type = %s
    AND status= 'dispo';
        """
        cursor.execute(query, (next_day_str, next_day_str, truck_type))
        free_trucks = [{'matricule': row[0], 'type': row[1]} for row in cursor.fetchall()]
        print("Executed SQL query:", cursor.statement)
        return jsonify({'free_trucks': free_trucks})

    except mysql.connector.Error as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to fetch free trucks'}), 500

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()








@app.route('/change_password', methods=['POST'])
@cross_origin(supports_credentials=True)

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



@app.route('/change_password2', methods=['POST'])
@cross_origin(supports_credentials=True)
def change_password2():
    if request.method == 'POST':
        try:
            username = request.json['username']
            old_password = request.json['old_password']
            new_password = request.json['new_password']

            # Authenticate the user
            if authenticate_user(username, old_password):
                connection = mysql.connector.connect(**db_config)
                cursor = connection.cursor()

                # Update the password in the database
                query = "UPDATE users SET Password = %s WHERE Username = %s"
                cursor.execute(query, (new_password, username))
                connection.commit()

                return jsonify({'status': 'success', 'message': 'Password changed successfully'})
            else:
                return jsonify({'status': 'failure', 'message': 'Invalid old password'})

        except Exception as e:
            print("Error: {}".format(e))
            return jsonify({'status': 'failure', 'message': 'Failed to change password'})








@app.route('/users', methods=['GET', 'DELETE'])
@cross_origin(supports_credentials=True)
def manage_users():
    if request.method == 'GET':
        # Fetch all users
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            query = "SELECT * FROM users"
            cursor.execute(query)
            users = [{'id': row[0], 'username': row[1], 'email': row[2], 'role': row[4]} for row in cursor.fetchall()]
            return jsonify({'users': users}), 200
        except mysql.connector.Error as error:
            print("Error: {}".format(error))
            return jsonify({'error': 'Failed to fetch users'}), 500
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()

    elif request.method == 'DELETE':
        # Delete a user
        user_id = request.args.get('id')
        if not user_id:
            return jsonify({'error': 'User ID is required for deletion'}), 400
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            delete_query = "DELETE FROM users WHERE idusers = %s"
            cursor.execute(delete_query, (user_id,))

            connection.commit()
            print("Executed SQL query:", cursor.statement)

            return jsonify({'message': 'User deleted successfully'}), 200
        except mysql.connector.Error as error:
            print("Error: {}".format(error))
            return jsonify({'error': 'Failed to delete user'}), 500
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()



@app.route('/Vehicules', methods=['GET', 'DELETE'])
@cross_origin(supports_credentials=True)
def manage_vehicules():
    if request.method == 'GET':
        # Fetch all users
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            query = "SELECT * FROM trucks"
            cursor.execute(query)
            vehicules = [{'matricule': row[0], 'type': row[1]} for row in cursor.fetchall()]
            return jsonify({'vehicules': vehicules}), 200
        except mysql.connector.Error as error:
            print("Error: {}".format(error))
            return jsonify({'error': 'Failed to fetch vehicules'}), 500
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()

@app.route('/seetasksmecano', methods=['GET', 'DELETE'])
@cross_origin(supports_credentials=True)
def manage_mecano_tasks():
    if request.method == 'GET':
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            query = """
                SELECT mt.tasks, mt.date, mt.done, m.name
                FROM mecano_tasks mt
                INNER JOIN mecano m ON mt.id_mecano = m.id
            """
            cursor.execute(query)
            tasks = [{'tasks': row[0], 'date': row[1], 'done': row[2], 'username': row[3]} for row in cursor.fetchall()]
            return jsonify({'tasks_mecano': tasks}), 200
        except mysql.connector.Error as error:
            print("Error: {}".format(error))
            return jsonify({'error': 'Failed to fetch mecano tasks'}), 500
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()




@app.route('/seetasksdriver', methods=['GET', 'DELETE'])
@cross_origin(supports_credentials=True)
def manage_driver_tasks():
    if request.method == 'GET':
        try:
            connection = mysql.connector.connect(**db_config)
            cursor = connection.cursor()
            query = """
                SELECT dt.task, dt.date ,dt.matricule , dt.done, d.username
                FROM driver_tasks dt
                INNER JOIN drivers d ON dt.id_driver = d.id
            """
            cursor.execute(query)
            tasks = [{'tasks': row[0], 'date': row[1], 'matricule': row[2], 'done': row[3], 'username': row[4] } for row in cursor.fetchall()]
            return jsonify({'tasks_mecano': tasks}), 200
        except mysql.connector.Error as error:
            print("Error: {}".format(error))
            return jsonify({'error': 'Failed to fetch mecano tasks'}), 500
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()




@app.route('/update-pickup-qr-code', methods=['POST'])
@cross_origin(supports_credentials=True)
def update_pickup_qr_code():
    data = request.json
    matricule = data.get('matricule')
    qr_code = data.get('qr_code')

    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Update or insert QR code blob into pickup table
        query = """
        INSERT INTO pickup (matricule, qrcode)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE qrcode = VALUES(qrcode)
        """
        cursor.execute(query, (matricule, qr_code))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'QR code updated successfully'})

    except Exception as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to update QR code'}), 500



@app.route('/update-truck-qr-code', methods=['POST'])
@cross_origin(supports_credentials=True)
def update_truck_qr_code():
    data = request.json
    matricule = data.get('matricule')
    qr_code = data.get('qr_code')

    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Update or insert QR code blob into pickup table
        query = """
        INSERT INTO truck (matricule, qrcode)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE qrcode = VALUES(qrcode)
        """
        cursor.execute(query, (matricule, qr_code))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'QR code updated successfully'})

    except Exception as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to update QR code'}), 500




@app.route('/update-semi-qr-code', methods=['POST'])
@cross_origin(supports_credentials=True)
def update_semi_qr_code():
    data = request.json
    matricule = data.get('matricule')
    qr_code = data.get('qr_code')

    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Update or insert QR code blob into pickup table
        query = """
        INSERT INTO semi (matricule, qrcode)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE qrcode = VALUES(qrcode)
        """
        cursor.execute(query, (matricule, qr_code))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'QR code updated successfully'})

    except Exception as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to update QR code'}), 500





@app.route('/fetch-qr-codes', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_all_qr_codes():
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Query to fetch all matricule and qrcode data from the pickup table
        query = "SELECT matricule, qrcode FROM pickup"
        cursor.execute(query)

        qr_codes = []
        for row in cursor.fetchall():
            matricule = row[0]
            qr_code_data = row[1]  # QR code data fetched as it is from the database

            # Encode QR code data as Base64
            qr_code_base64 = base64.b64encode(qr_code_data).decode('utf-8')

            qr_codes.append({'matricule': matricule, 'qr_code': qr_code_base64})

        # Return all fetched data as a list of dictionaries
        return jsonify({'qr_codes': qr_codes})

    except mysql.connector.Error as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to fetch QR codes'}), 500



    
    
    
@app.route('/profile', methods=['GET'])
@cross_origin(supports_credentials=True)
def get_profile():
    # Retrieve username from query parameter
    username = request.args.get('username')
    print('aaaaa', username)

    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        query = "SELECT Username, Mail, Role FROM users WHERE Username = %s"
        cursor.execute(query, (username,))
        profile_data = cursor.fetchone()
        print("Executed SQL query:", cursor.statement)

        if not profile_data:
            return jsonify({'error': 'Profile not found'}), 404

        profile = {
            'name': profile_data[0],
            'Mail': profile_data[1],
            'Role': profile_data[2]
        }
        print(profile)
        return jsonify({'profile': profile}), 200

    except mysql.connector.Error as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to fetch profile'}), 500
           
    
    
    
@app.route('/report', methods=['POST'])
@cross_origin(supports_credentials=True)
def save_report():
    vehicle_id = request.form.get('vehicleId')
    issue_description = request.form.get('issueDescription')
    work_description = request.form.get('workDescription')
    signature = request.form.get('signature')
    username = request.form.get('username')

    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Retrieve the mechanic ID from the database using the username
        query_mecano = """
        SELECT id FROM mecano WHERE name = %s
        """
        cursor.execute(query_mecano, (username,))
        mecano_id = cursor.fetchone()[0]  # Assuming it returns a single ID

        # Insert the report data into the mecano_reports table
        query = """
        INSERT INTO mecano_reports (`id-trucks`, `id-mecano`, `username`, `issue_description`, `work_description`, `signature`)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (vehicle_id, mecano_id, username, issue_description, work_description, signature))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Report saved successfully'})

    except Exception as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to save report'}), 500

    
  


@app.route('/register_tokendriver', methods=['POST'])
def register_tokenDriver():
    username = request.form.get('username')
    token = request.form.get('deviceToken')
    print("token",token)
    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Update the chef record with the token
        query = """
        UPDATE drivers SET token = %s WHERE username = %s
        """
        cursor.execute(query, (token, username))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Token registered successfully'}), 200

    except Exception as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to register token'}), 500
  
   
  
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()




@app.route('/register_tokenmecano', methods=['POST'])
def register_tokenmecano():
    username = request.form.get('username')
    token = request.form.get('deviceToken')
    print("token",token)
    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Update the chef record with the token
        query = """
        UPDATE mecano SET token = %s WHERE name = %s
        """
        cursor.execute(query, (token, username))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Token registered successfully'}), 200

    except Exception as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to register token'}), 500
  
   
  
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()




@app.route('/register_token', methods=['POST'])
def register_token():
    username = request.form.get('username')
    token = request.form.get('deviceToken')
    print("token",token)
    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        # Update the chef record with the token
        query = """
        UPDATE chef SET token = %s WHERE username = %s
        """
        cursor.execute(query, (token, username))
        connection.commit()

        cursor.close()
        connection.close()

        return jsonify({'message': 'Token registered successfully'}), 200

    except Exception as error:
        print("Error: {}".format(error))
        return jsonify({'error': 'Failed to register token'}), 500
  
   
  
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()




@app.route('/send_notification', methods=['POST'])
def send_notification():
    # Get the device token and notification message from the request
    username = request.form.get('username')
    title = request.form.get('title')
    message = request.form.get('message')
    mecano = request.form.get('mecano')
    # Construct the FCM payload
    t=title+" by "+mecano;
   
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
   
    query_token = """
    SELECT token FROM chef WHERE username = %s
    """
    cursor.execute(query_token, (username,))
    device_token= cursor.fetchone()[0]
    print("Executed SQL query:", cursor.statement)
    print("token",device_token)
    
    query_insert_notification = """
       INSERT INTO notifications (username,title, content) VALUES (%s, %s, %s)
       """
    cursor.execute(query_insert_notification, (username, t, message))

       # Commit the transaction
    connection.commit()
    
    
    
    
    fcm_payload = {
        "to": device_token,
        "notification": {
        "title": t,
        "body": message
        }
    }

    # Add any additional data if required
    # fcm_payload["data"] = {...}

    # Send the notification using Firebase Cloud Messaging (FCM)
    response = requests.post('https://fcm.googleapis.com/fcm/send', json=fcm_payload, headers={
        'Authorization': 'key=AAAAPTfojs4:APA91bEU75LGeU7UMnLP0WKUuCK07KLnGeeG0IhZzvtlMr882nuRUMR1bTFLdIgMCKJJvYgkLGyDxydi9Nj9S01RSBsX6wKOTsyfE6APFUAFq16vE_uiaV-JNILDWQ-uJUEq28qt3NFO',  # Replace YOUR_SERVER_KEY with your FCM server key
        'Content-Type': 'application/json'
    })

    # Check the response from FCM
    if response.status_code == 200:
        return jsonify({'message': 'Notification sent successfully'}), 200
    else:
        return jsonify({'error': 'Failed to send notification'}), response.status_code


@app.route('/send_notificationdriver', methods=['POST'])
@cross_origin(supports_credentials=True)

def send_notificationdriver():
    data = request.json
    username = data.get('username')
    title = data.get('title')
    message = data.get('message')
    print(username)
   
   
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
   
    query_token = """
    SELECT token FROM drivers WHERE id = %s
    """
    cursor.execute(query_token, ('36',))
    device_token= cursor.fetchone()[0]
    print("Executed SQL query:", cursor.statement)
    print("token",device_token)
    
    query_insert_notification = """
       INSERT INTO notifications (username,title, content) VALUES (%s, %s, %s)
       """
    cursor.execute(query_insert_notification, (username, title, message))

       # Commit the transaction
    connection.commit()
    
    
    
    
    fcm_payload = {
        "to": device_token,
        "notification": {
        "title": title,
        "body": message
        }
    }

    # Add any additional data if required
    # fcm_payload["data"] = {...}

    # Send the notification using Firebase Cloud Messaging (FCM)
    response = requests.post('https://fcm.googleapis.com/fcm/send', json=fcm_payload, headers={
        'Authorization': 'key=AAAAPTfojs4:APA91bEU75LGeU7UMnLP0WKUuCK07KLnGeeG0IhZzvtlMr882nuRUMR1bTFLdIgMCKJJvYgkLGyDxydi9Nj9S01RSBsX6wKOTsyfE6APFUAFq16vE_uiaV-JNILDWQ-uJUEq28qt3NFO',  # Replace YOUR_SERVER_KEY with your FCM server key
        'Content-Type': 'application/json'
    })

    # Check the response from FCM
    if response.status_code == 200:
        return jsonify({'message': 'Notification sent successfully'}), 200
    else:
        return jsonify({'error': 'Failed to send notification'}), response.status_code











@app.route('/send_notificationmecano', methods=['POST'])
@cross_origin(supports_credentials=True)

def send_notificationmecano():
    data = request.json
    username = data.get('username')
    title = data.get('title')
    message = data.get('message')
    print(username)
   
   
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
   
    query_token = """
    SELECT token FROM mecano WHERE id = %s
    """
    cursor.execute(query_token, (username,))
    device_token= cursor.fetchone()[0]
    print("Executed SQL query:", cursor.statement)
    print("token",device_token)
    
    query_insert_notification = """
       INSERT INTO notifications (username,title, content) VALUES (%s, %s, %s)
       """
    cursor.execute(query_insert_notification, (username, title, message))

       # Commit the transaction
    connection.commit()
    
    
    
    
    fcm_payload = {
        "to": device_token,
        "notification": {
        "title": title,
        "body": message
        }
    }

    # Add any additional data if required
    # fcm_payload["data"] = {...}

    # Send the notification using Firebase Cloud Messaging (FCM)
    response = requests.post('https://fcm.googleapis.com/fcm/send', json=fcm_payload, headers={
        'Authorization': 'key=AAAAPTfojs4:APA91bEU75LGeU7UMnLP0WKUuCK07KLnGeeG0IhZzvtlMr882nuRUMR1bTFLdIgMCKJJvYgkLGyDxydi9Nj9S01RSBsX6wKOTsyfE6APFUAFq16vE_uiaV-JNILDWQ-uJUEq28qt3NFO',  # Replace YOUR_SERVER_KEY with your FCM server key
        'Content-Type': 'application/json'
    })

    # Check the response from FCM
    if response.status_code == 200:
        return jsonify({'message': 'Notification sent successfully'}), 200
    else:
        return jsonify({'error': 'Failed to send notification'}), response.status_code

























@app.route('/Get_notifications', methods=['GET'])
@cross_origin(supports_credentials=True)

def get_notifications():
    # Get the username from the request
    username = request.args.get('username')
    # Connect to MySQL database
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Fetch notifications for the specified username
    query_fetch_notifications = """
    SELECT title, content FROM notifications WHERE username = %s
    """
    cursor.execute(query_fetch_notifications, (username,))
    notifications = cursor.fetchall()

    # Close database cursor
    cursor.close()

    # Transform notifications into a list of dictionaries
    notification_list = []
    for notification in notifications:
        notification_list.append({'title': notification[0], 'content': notification[1]})

    # Return notifications as JSON response
    return jsonify(notification_list)



@app.route('/saveRehla', methods=['POST'])
def save_Rehla():
    username = request.form.get('username')
    addresses = request.form.get('addresses')
    km = request.form.get('km')
    
    # Connect to MySQL database
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        print(f"Connected to database for user: {username}")
        
        # Get the user's ID based on the username
        query_get_id = "SELECT id FROM drivers WHERE username = %s"
        cursor.execute(query_get_id, (username,))
        user_id = cursor.fetchone()
        
        if user_id is None:
            return f"Error: User {username} not found", 404
        
        user_id = user_id[0]  # Fetch the first column of the first row
        
        print(f"User ID for {username} is {user_id}")
        
        # Get the task_id from driver_tasks for the given user_id and today's date
        today_date = date.today()  # Get today's date
        query_get_task_id = "SELECT idtask FROM driver_tasks WHERE id_driver = %s AND Date = %s"
        cursor.execute(query_get_task_id, (user_id, today_date))
        task_id = cursor.fetchone()
        
        if task_id is None:
            task_id = None
        else:
            task_id = task_id[0]  # Fetch the first column of the first row
        
        print(f"Task ID for user {username} on {today_date} is {task_id}")
        
        # Insert data into the rehla table
        query_insert_rehla = "INSERT INTO rehla (id_D, date, destinations, km, id_task) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(query_insert_rehla, (user_id, today_date, addresses, km, task_id))
        
        # Commit the transaction
        connection.commit()
        
        print(f"Rehla entry saved for user {username}")
        
        return "Rehla saved successfully", 200
    except Exception as e:
        print(f"Error: {e}")
        connection.rollback()  # Rollback the transaction if an error occurs
        return f"Error: {e}", 500
    



@app.route("/get-rehla", methods=['GET'])
def get_Rehla():
    username = request.args.get('username')
    print('username=', username)

    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        query_get_id = "SELECT id FROM drivers WHERE username = %s"
        cursor.execute(query_get_id, (username,))
        user_id = cursor.fetchone()
        cursor.fetchall()  # Clear any remaining results
        
        if not user_id:
            print("User not found")
            return jsonify({'message': 'User not found'}), 404
        
        user_id = user_id[0]
        today_date = date.today()
        print('user_id=', user_id, 'today_date=', today_date)
        
        query_get_rehla = "SELECT destinations FROM rehla WHERE id_D = %s AND date = %s"
        cursor.execute(query_get_rehla, (user_id, today_date))
        destinations_result = cursor.fetchone()
        cursor.fetchall()  # Clear any remaining results

        if not destinations_result:
            print("No destinations found for today")
            return jsonify({'message': 'No destinations found for today'}), 404
        
        destinations_str = destinations_result[0]
        destinations_list = destinations_str.split(',')
        
        return jsonify(destinations_list), 200

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return jsonify({'message': str(err)}), 500

    finally:
        try:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        except mysql.connector.Error as err:
            print(f"Error closing connection: {err}")

    
    
@app.route('/getMonthlyDistance', methods=['GET'])
def get_monthly_distance():
    username = request.args.get('username')
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    
    # Get the user's ID based on the username
    query_get_id = "SELECT id FROM drivers WHERE username = %s"
    cursor.execute(query_get_id, (username,))
    user_id = cursor.fetchone()

    if user_id:
        user_id = user_id[0]
        
        query_get_distance = """
            SELECT DATE_FORMAT(date, '%Y-%m') as month, SUM(km) as total_km
            FROM rehla
            WHERE id_D = %s
            GROUP BY DATE_FORMAT(date, '%Y-%m')
            ORDER BY DATE_FORMAT(date, '%Y-%m')
        """
        cursor.execute(query_get_distance, (user_id,))
        result = cursor.fetchall()
        
        monthly_data = [{"month": row[0], "total_km": row[1]} for row in result]
        
        cursor.close()
        connection.close()
        print(monthly_data)
        return jsonify(monthly_data), 200
    else:
        cursor.close()
        connection.close()
        return jsonify({'message': 'User not found'}), 404
    




@app.route('/mechanicTaskCounts', methods=['GET'])
def mechanic_task_counts():
    username = request.args.get('username')
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Get the mechanic ID based on the username
    query_get_id = "SELECT id FROM mecano WHERE name = %s"
    cursor.execute(query_get_id, (username,))
    user_id = cursor.fetchone()

    if user_id:
        user_id = user_id[0]

        # Get the counts of maintenance and repair tasks for the mechanic
        query_get_task_counts = """
            SELECT task_type, COUNT(*) AS count
            FROM mecano_tasks
            WHERE id_mecano = %s
            GROUP BY task_type
        """
        cursor.execute(query_get_task_counts, (user_id,))
        result = cursor.fetchall()

        task_counts = [{"task_type": row[0], "count": row[1]} for row in result]

        cursor.close()
        connection.close()

        return jsonify(task_counts), 200
    else:
        cursor.close()
        connection.close()
        return jsonify({'message': 'User not found'}), 404


@app.route('/taskDoneRatio', methods=['GET'])
def task_done_ratio():
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Query to get the ratio of done tasks per date
    query_get_done_ratio = """
        SELECT date,
               SUM(CASE WHEN done = 'yes' THEN 1 ELSE 0 END) / COUNT(*) AS done_ratio
        FROM mecano_tasks
        GROUP BY date
        ORDER BY date
    """
    cursor.execute(query_get_done_ratio)
    result = cursor.fetchall()

    done_ratios = [{"date": row[0], "done_ratio": row[1]} for row in result]

    cursor.close()
    connection.close()

    return jsonify(done_ratios), 200


@app.route('/getDestinationCounts', methods=['GET'])
def get_destination_counts():
    username = request.args.get('username')
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    
    query_get_id = "SELECT id FROM drivers WHERE username = %s"
    cursor.execute(query_get_id, (username,))
    user_id = cursor.fetchone()

    if user_id:
        user_id = user_id[0]
        
        query_get_destinations = """
            SELECT destinations
            FROM rehla
            WHERE id_D = %s
        """
        cursor.execute(query_get_destinations, (user_id,))
        result = cursor.fetchall()
        
        destination_counts = {}
        for row in result:
            destinations = row[0].split(',')
            for destination in destinations:
                if destination in destination_counts:
                    destination_counts[destination] += 1
                else:
                    destination_counts[destination] = 1
        
        destination_data = [{"destination": key, "count": value} for key, value in destination_counts.items()]
        
        cursor.close()
        connection.close()
        
        return jsonify(destination_data), 200
    else:
        cursor.close()
        connection.close()
        return jsonify({'message': 'User not found'}), 404
#--------------------------------------------------------- DASHBOARD --------------------------------------------------------
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('mysql+mysqlconnector://me:0000@localhost/dwh4')

@app.route('/active-users-count')
@cross_origin(supports_credentials=True)
def active_users_count():
    query = "SELECT COUNT(*) AS active_users_count FROM dimusers WHERE status = 'active';"
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))

@app.route('/inactive-users-count')
@cross_origin(supports_credentials=True)
def inactive_users_count():
    query = "SELECT COUNT(*) AS inactive_users_count FROM dimusers WHERE status = 'inactive';"
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))

@app.route('/new-users-count-by-date')
@cross_origin(supports_credentials=True)
def new_users_count_by_date():
    query = "SELECT hire_date, COUNT(*) AS new_users_count FROM user_management_fact GROUP BY hire_date ORDER BY hire_date;"
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))

@app.route('/user-activity-count')
@cross_origin(supports_credentials=True)
def user_activity_count():
    query = "SELECT user_id, activity_count FROM user_management_fact;"
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))



@app.route('/daily-active-users')
@cross_origin(supports_credentials=True)
def daily_active_users():
    query = """
    SELECT t.date_key, COUNT(fum.user_id) AS daily_active_users
    FROM time_dimension t
    JOIN user_management_fact fum ON t.date_key = fum.last_time_active
    WHERE fum.status = 'active'
    GROUP BY t.date_key
    ORDER BY t.date_key;
    """
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))



@app.route('/average-activity-per-day')
@cross_origin(supports_credentials=True)
def average_activity_per_day():
    query = "SELECT user_id, AVG(activity_count) AS average_activity_per_day FROM user_management_fact GROUP BY user_id;"
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))






@app.route('/active-to-total-users-ratio')
@cross_origin(supports_credentials=True)
def active_to_total_users_ratio():
    active_query = "SELECT COUNT(*) AS active_users_count FROM dimusers WHERE status = 'active';"
    total_query = "SELECT COUNT(*) AS total_users_count FROM dimusers;"
    
    active_result = pd.read_sql(active_query, engine)
    total_result = pd.read_sql(total_query, engine)
    
    active_users_count = int(active_result.iloc[0]['active_users_count'])
    total_users_count = int(total_result.iloc[0]['total_users_count'])
    
    inactive_users_count = total_users_count - active_users_count
    
    data = {
        "active_users_count": active_users_count,
        "inactive_users_count": inactive_users_count,
        "total_users_count": total_users_count,
    }
    
    return jsonify(data)



# New Endpoints for Vehicle Maintenance Fact Table ------------------------------------------------------------------

@app.route('/vehicle-maintenance-fact')
@cross_origin(supports_credentials=True)
def vehicle_maintenance_fact():
    query = """
    SELECT 
        vehicle_id, 
        last_maintenance_date_key, 
        next_maintenance_date_key, 
        last_repared_at_key, 
        maintenance_interval, 
        time_since_last_repair 
    FROM vehicle_maintenance_fact;
    """
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))

@app.route('/vehicle-maintenance-count')
@cross_origin(supports_credentials=True)
def vehicle_maintenance_count():
    query = """
    SELECT 
        vehicle_id, 
        COUNT(*) AS maintenance_count 
    FROM 
        vehicle_maintenance_fact 
    GROUP BY 
        vehicle_id;
    """
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))


@app.route('/vehicle-maintenance-by-date')
@cross_origin(supports_credentials=True)
def vehicle_maintenance_by_date():
    query = """
    SELECT 
        last_maintenance_date_key AS date, 
        COUNT(*) AS maintenance_count 
    FROM vehicle_maintenance_fact 
    GROUP BY last_maintenance_date_key 
    ORDER BY last_maintenance_date_key;
    """
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))

@app.route('/vehicle-repair-time')
@cross_origin(supports_credentials=True)
def vehicle_repair_time():
    query = """
    SELECT 
        vehicle_id, 
        time_since_last_repair 
    FROM vehicle_maintenance_fact;
    """
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))





@app.route('/vehicle-maintenance-by-vehicle/<vehicle_id>')
@cross_origin(supports_credentials=True)
def vehicle_maintenance_by_vehicle(vehicle_id):
    query = f"""
    SELECT 
        vehicle_id, 
        last_maintenance_date_key, 
        maintenance_interval 
    FROM vehicle_maintenance_fact 
    WHERE vehicle_id = '{vehicle_id}';
    """
    result = pd.read_sql(query, engine)
    # Ensure last_maintenance_date_key is in a format that JavaScript can easily parse
    result['last_maintenance_date_key'] = result['last_maintenance_date_key'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)
    print(result)
    print('aaaaaaaaaaaa')
    return jsonify(result.to_dict(orient='records'))




@app.route('/vehicle-maintenance-count-model')
@cross_origin(supports_credentials=True)
def vehicle_maintenance_count_model():
    query = """
    SELECT 
        dv.model, 
        COUNT(vmf.vehicle_id) AS maintenance_count 
    FROM 
        vehicle_maintenance_fact vmf
    JOIN 
        dim_vehicles dv 
    ON 
        vmf.vehicle_id = dv.vehicle_id 
    GROUP BY 
        dv.model;
    """
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))





@app.route('/vehicle-status-count')
@cross_origin(supports_credentials=True)
def vehicle_status_count():
    query = """
    SELECT 
        status, 
        COUNT(*) AS status_count 
    FROM 
        dim_vehicles 
    GROUP BY 
        status;
    """
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))

# Endpoints for driver tasks ----------------------------------------------------------------
@app.route('/drivers')
@cross_origin(supports_credentials=True)
def get_drivers():
    query = """
    SELECT 
        * 
    FROM 
        dimusers 
    WHERE 
        role = 'driver';
    """
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))

@app.route('/driver-tasks', methods=['GET'])
@cross_origin(supports_credentials=True)
def driver_tasks():
    query = """
   SELECT 
        f.user_id,
        u.username,
        t.task_id,
        t.description AS task_description,
        f.KM_covered,
        f.destinations,
        f.vehicle_id,
        f.task_date_key
    FROM 
        fact_driver_tasks f
    JOIN 
        task_dimension t ON f.task_id = t.task_id
    JOIN 
        dimusers u ON f.user_id = u.user_id
    WHERE 
        t.task_for = 'driver';
    """
    result = pd.read_sql(query, engine)
    print('aaaaaaaaaaaaaaaa')
    print(result)
    return jsonify(result.to_dict(orient='records'))

@app.route('/total-km-covered', methods=['GET'])
@cross_origin(supports_credentials=True)
def total_km_covered():
    query = """
    SELECT 
        f.user_id,
        u.username,
        SUM(f.KM_covered) AS total_km_covered
    FROM 
        fact_driver_tasks f
    JOIN 
        dimusers u ON f.user_id = u.user_id
    GROUP BY 
        f.user_id, u.username
    """
    result = pd.read_sql(query, engine)
    return jsonify(result.to_dict(orient='records'))


from collections import Counter
@app.route('/destinations-count', methods=['GET'])
@cross_origin(supports_credentials=True)

def destinations_count():
    query = "SELECT destinations FROM fact_driver_tasks;"
    df = pd.read_sql(query, engine)

    # Flatten and count destinations
    all_destinations = []
    for destinations in df['destinations']:
        if destinations:
            all_destinations.extend(destinations.split(','))

    destination_counts = Counter(all_destinations)
    result = [{"destination": dest, "count": count} for dest, count in destination_counts.items()]

    return jsonify(result)
# Mecano fact endpoints -------------------------------------------


def execute_query(query):
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result.to_dict(orient='records')




@app.route('/mechanic-performance', methods=['GET'])
@cross_origin(supports_credentials=True)

def mechanic_performance():
    query = """
    SELECT 
        f.user_id,
        u.username,
        f.total_tasks,
        f.total_done_tasks,
        f.percentage_tasks_done
    FROM 
        fact_mecanotask f
    JOIN 
        dimusers u ON f.user_id = u.user_id
    GROUP BY 
        f.user_id, u.username, f.total_tasks, f.total_done_tasks, f.percentage_tasks_done
    """
    result = execute_query(query)
    return jsonify(result)

@app.route('/vehicle-model-performance', methods=['GET'])
@cross_origin(supports_credentials=True)

def vehicle_model_performance():
    query = """
    SELECT 
    f.user_id,
    u.username,
    v.model AS vehicle_model,
    f.tasks_done_per_model,
    f.total_tasks_per_model,
    f.percentage_tasks_done_per_model
FROM 
    fact_mecanotask f
JOIN 
    dimusers u ON f.user_id = u.user_id
JOIN 
    dim_vehicles v ON f.vehicle_id = v.vehicle_id
GROUP BY 
    f.user_id, u.username, v.model, f.tasks_done_per_model, f.total_tasks_per_model, f.percentage_tasks_done_per_model
"""
    result = execute_query(query)
    return jsonify(result)

@app.route('/task-details', methods=['GET'])
@cross_origin(supports_credentials=True)

def task_details():
    query = """
    SELECT 
        t.task_id,
        t.description,
        t.task_for,
        t.task_type,
        t.done
    FROM 
        task_dimension t
        WHERE t.task_for= 'mecano'
    """
    result = execute_query(query)
    return jsonify(result)

@app.route('/tasks-done-over-time', methods=['GET'])
@cross_origin(supports_credentials=True)

def tasks_done_over_time():
    query = """
    SELECT 
        f.task_date_key,
        COUNT(CASE WHEN f.total_done_tasks > 0 THEN 1 END) AS done_tasks,
        COUNT(*) AS total_tasks
    FROM 
        fact_mecanotask f
    GROUP BY 
        f.task_date_key
    """
    result = execute_query(query)
    return jsonify(result)



if __name__ == '__main__':
    app.run(host='172.20.10.13', port=5001, debug=True)