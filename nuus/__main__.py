if __name__ == '__main__':
    from nuus import app, init_app
    from nuus import servers
    init_app()
    servers.run_tornado(app,debug=True)
