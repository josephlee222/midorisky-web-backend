from datetime import date, datetime

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, bytes):
        # 0 = false, 1 = true
        if obj == b'\x01':
            return True
        elif obj == b'\x00':
            return False
        return obj.decode('utf-8')




    raise TypeError ("Type %s not serializable" % type(obj))