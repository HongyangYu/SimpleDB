Run the following commands to do tests.

HeapFileReadTest:
ant runtest -Dtest=HeapFileReadTest

HeapPageReadTest:
ant runtest -Dtest=HeapPageReadTest

PinCountEvictionTest:
ant runtest -Dtest=systemtest.PinCountEvictionTest

PinCountTest:
ant runtest -Dtest=systemtest.PinCountTest

BPPinCountTest:
ant runtest -Dtest=systemtest.BPPinCountTest
