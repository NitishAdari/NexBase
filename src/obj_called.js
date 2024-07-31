import NEXBASE from "nexbase-db";

const nexbaseInstance = new NEXBASE(
    "postgresql://localhost:8000/test1", 
    "postgresql://localhost:8001/test1", 
    "postgresql://localhost:8002/test1", 
    "postgresql://localhost:8003/test1"
);

async function exampleUsage() {
    try {
        // Connect to the database and initialize tables
        await nexbaseInstance.connect();
        console.log('Connected and tables initialized.');

        // Set a key-value pair with a TTL of 3600 seconds
        const setResult = await nexbaseInstance.addToTaskQueue('set', 'key1', 'val1', 3600);
        console.log('Set result:', setResult);

        // Get the value associated with the key
        const getResult = await nexbaseInstance.addToTaskQueue('get', 'key1');
        console.log('Get result:', getResult);

        // Get the value multiple times
        for (let i = 0; i < 5; i++) {
            const repeatedGetResult = await nexbaseInstance.addToTaskQueue('get', 'key1');
            console.log(`Repeated Get result ${i + 1}:`, repeatedGetResult);
        }

        // Set a new expiration time for the key
        const set_expiry = await nexbaseInstance.addToTaskQueue('expire', 'key1', null, 1800);
        console.log('Expire result:', expireResult);

        // Delete the key-value pair
        const del_result = await nexbaseInstance.addToTaskQueue('delete', 'key1');
        console.log('Delete result:', del_result);

        const allkeys = await nexbaseInstance.addToTaskQueue('getAllKeys', 'key1');
        console.log('All Keys in DB(s) result:', allkeys);

        const countkeys = await nexbaseInstance.addToTaskQueue('CountKeys');
        console.log('No. of Keys result:', countkeys);

        const del_all_expired = await nexbaseInstance.addToTaskQueue('deleteAllExpired');

    } catch (err) {
        console.error('Error:', err);
    } finally {
        // Disconnect from the database
        await nexbaseInstance.disconnect();
        console.log('Disconnected from the database.');
    }
}

// Run the example
exampleUsage();
