python main.py &
PRODUCER_PID=$!

sleep 1 # wait for metadata to be available.
cargo run

kill $PRODUCER_PID
