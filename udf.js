function transform(line) {
    var values = line.split(',');

    // Check if the line is the header by checking the first value
    if (values[0].toLowerCase() === 'sale_id') {
        return null; // Ignore the header row
    }

    var obj = new Object();
    obj.sale_id = values[0];
    obj.sale_date = values[1];
    obj.amount = values[2];
    obj.customer_id = values[3];
    var jsonString = JSON.stringify(obj);
    return jsonString;
}

			
