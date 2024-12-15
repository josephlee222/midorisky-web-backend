from chalice import Chalice
from chalicelib.userRoutes import user_routes
from chalicelib.farmRoutes import farm_routes
from chalicelib.authorizers import auth_functions, admin_authorizer



app = Chalice(app_name='midorisky')
app.register_blueprint(user_routes)
app.register_blueprint(farm_routes)
app.register_blueprint(auth_functions)


@app.route('/')
def index():
    return {'message': 'Hello world, from MidoriSKY!'}

@app.route('/test/admin', authorizer=admin_authorizer)
def test_admin():
    return {'message': 'You have access to admin routes!'}


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
