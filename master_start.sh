broadcast_ip=$(ip addr show | grep brd | grep 255 | cut -c30-42)
echo $broadcast_ip
