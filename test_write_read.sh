#!/bin/bash

cd /home/ubuntu/rapid-transfer/build

# Kill any existing file_transfer processes
killall -9 file_transfer 2>/dev/null
sleep 1

# Start receiver in background
echo "Starting receiver..."
./example/file_transfer --role=receiver --use_write_read=1 --listen=localhost:12359 --device=ibp51s0 > /tmp/receiver.log 2>&1 &
RECEIVER_PID=$!
echo "Receiver PID: $RECEIVER_PID"

# Wait for receiver to start
sleep 3

# Get buffer address from receiver log
BUFFER_ADDR=$(grep "Receiver buffer address" /tmp/receiver.log | awk '{print $NF}')
echo "Buffer address: $BUFFER_ADDR"

if [ -z "$BUFFER_ADDR" ]; then
    echo "Failed to get buffer address"
    cat /tmp/receiver.log
    kill $RECEIVER_PID 2>/dev/null
    exit 1
fi

# Start sender
echo "Starting sender..."
./example/file_transfer --role=sender --use_write_read=1 \
    --target=localhost:12359 \
    --remote_addr=$BUFFER_ADDR \
    --remote_rkey=0 \
    --path=/tmp/test_file.txt \
    --device=ibp51s0 2>&1 | tee /tmp/sender.log

SENDER_EXIT=$?

# Wait a bit
sleep 2

# Check receiver log
echo "=== Receiver log ==="
cat /tmp/receiver.log

# Kill receiver
kill $RECEIVER_PID 2>/dev/null

if [ $SENDER_EXIT -eq 0 ]; then
    echo "=== Test PASSED ==="
else
    echo "=== Test FAILED ==="
fi

