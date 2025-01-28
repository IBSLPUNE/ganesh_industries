import frappe
from frappe.utils import nowdate, nowtime
from frappe import _

@frappe.whitelist()
def get_next_op_batch(batch_no):
    # Ensure batch_no is provided
    if not batch_no:
        frappe.throw(_("Batch number is required"), frappe.MandatoryError)

    # Fetch the maximum custom_op_batch value for the batch where docstatus = 1 (submitted only)
    result = frappe.db.sql("""
        SELECT MAX(CAST(custom_op_batch AS UNSIGNED))
        FROM `tabStock Entry Detail`
        WHERE batch_no = %s AND docstatus = 1
    """, (batch_no,))
    
    max_op_batch = result[0][0] if result and result[0][0] is not None else 0

    # Calculate the next serial number (if no max_op_batch found, start from 1)
    return max_op_batch + 1


@frappe.whitelist()
def on_update(self, method):
    # Only process for submitted Stock Entries (docstatus=1)
    if self.docstatus != 1:
        return

    # Debugging: Log the stock entry type
    frappe.log("Stock Entry Type: {0}".format(self.stock_entry_type))
    
    # Update custom_op_batch for items in Stock Entry
    for item in self.items:
        if not item.get("custom_op_batch"):
            # Fetch the next op batch number for the batch_no
            next_op_batch = get_next_op_batch(item.batch_no)
            
            # Set the custom_op_batch field for the item
            item.custom_op_batch = next_op_batch

    # Save changes to the Stock Entry (if any)
    if any(item.get("custom_op_batch") for item in self.items):
        self.save(ignore_permissions=True)

    # Check if stock entry type is 'Manufacture'
    if self.stock_entry_type == 'Manufacture':
        frappe.log("Updating OP Batch Table for Stock Entry: {0}".format(self.name))
        update_op_batch_table(self)


def update_op_batch_table(self, *args, **kwargs):
    # Fetch the batch number from the stock entry
    batch_no = self.items[0].batch_no  # 'self' refers to the Stock Entry instance
    
    # Check if stock entry type is 'Manufacture'
    if self.stock_entry_type != 'Manufacture':
        frappe.log("Skipping batch update for stock entry: {0} as type is not Manufacture.".format(self.name))
        return  # Skip updating the batch table if the stock entry type is not 'Manufacture'

    # Fetch the Batch document
    try:
        batch_doc = frappe.get_doc("Batch", batch_no)
    except frappe.DoesNotExistError:
        frappe.throw(_("Batch {0} does not exist").format(batch_no), frappe.DoesNotExistError)

    # Fetch all distinct Stock Entries linked to the batch
    stock_entries = frappe.get_all("Stock Entry Detail", filters={"batch_no": batch_no}, fields=["parent"], distinct=True)

    # Log the stock entries linked to the batch
    frappe.log("Stock Entries Linked to Batch {0}: {1}".format(batch_no, stock_entries))

    # Map existing entries in the 'op_batch_details_table' for quick lookup
    existing_entries = {row.stock_entry for row in batch_doc.get("op_batch_details_table", [])}

    # Keep track of the last 'op' value to ensure the next one is unique
    last_op_value = len(existing_entries)

    # Append missing Stock Entries to the Batch's OP Batch Table
    for entry in stock_entries:
        stock_entry_name = entry["parent"]
        if stock_entry_name not in existing_entries:
            try:
                # Fetch the Stock Entry document
                stock_entry_doc = frappe.get_doc("Stock Entry", stock_entry_name)

                # Check if the stock entry type is 'Manufacture'
                if stock_entry_doc.stock_entry_type == 'Manufacture':
                    # Iterate over items in the stock entry, but filter out finished items
                    for item in stock_entry_doc.items:
                        # Only process unfinished items (i.e., skip finished items)
                        if item.is_finished_item:  # This filters out finished items
                            continue  # Skip the iteration for finished items

                        # Only append rows where batch_no exists
                        if item.batch_no:
                            last_op_value += 1  # Increment to get the next unique value
                                
                            # Append the valid row to the op_batch_details_table
                            batch_doc.append("op_batch_details_table", {
                                "stock_entry": stock_entry_doc.name,
                                "stock_entry_date": stock_entry_doc.posting_date or nowdate(),
                                "stock_entry_time": stock_entry_doc.posting_time or nowtime(),
                                "op": item.custom_op_batch  # Use the 'custom_op_batch' value from the Stock Entry item
                            })
                else:
                    frappe.log("Skipping Stock Entry {0} as it's not of type 'Manufacture'.".format(stock_entry_name))
            except frappe.DoesNotExistError:
                frappe.throw(_("Stock Entry {0} does not exist").format(stock_entry_name), frappe.DoesNotExistError)

    # Save the updated Batch document
    batch_doc.save(ignore_permissions=True)