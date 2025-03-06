#!/bin/bash

# Function to handle termination signals
terminate() {
    echo "Terminating processes..."
    pkill -SIGTERM -P $$
    wait
    exit 0
}

# Trap termination signals
trap terminate SIGTERM SIGINT

# Start load_database.py in the background
python load_database.py &

# Start streamlit in the background
streamlit run app.py --server.port=8501 --server.address=0.0.0.0 &

# Wait for any process to exit
wait -n

# Exit with status of the process that exited first
exit $?
