(go test -run TestBasic 2>&1) |  grep -v RPC | grep -v dial | grep -v wrong | grep -v accept
