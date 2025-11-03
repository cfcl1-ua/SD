##########################################################
# ==== CLASE INTERFAZ: DASHBOARD WEB PARA MONITORIZAR LOS PUNTOS DE RECARGA ====
##########################################################

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import webbrowser
import threading
import time


class interfaz:
    """
    Clase que gestiona la interfaz web (Dash) para mostrar los
    puntos de recarga en tiempo real.
    No accede a ficheros ni a Kafka: solo recibe una lista de CPs.
    """

    def __init__(self, puerto=8050, intervalo=3, price_per_kwh=0.0):
        """
        puerto: puerto HTTP del dashboard
        intervalo: segundos entre refrescos automáticos
        price_per_kwh: precio del kWh que define la central
        """
        self.puerto = puerto
        self.intervalo = intervalo
        self.price_per_kwh = price_per_kwh
        self.app = None
        self._cps = []
        self._lock = threading.Lock()

    # -----------------------------------------------------
    # Método auxiliar: color según estado
    # -----------------------------------------------------
    @staticmethod
    def color_estado(estado):
        estado = estado.upper()
        if estado in ("IDLE", "DISPONIBLE"):
            return "green"
        elif estado in ("OCUPADO", "SUMINISTRANDO"):
            return "blue"
        elif estado in ("AVERIADO", "OUT OF ORDER", "OUT_OF_ORDER"):
            return "red"
        elif estado in ("DESCONECTADO", "INACTIVO"):
            return "gray"
        else:
            return "orange"

    # -----------------------------------------------------
    # Método público: actualizar lista de CPs desde la central
    # -----------------------------------------------------
    def actualizar(self, cps):
        """
        Actualiza la lista de puntos de recarga mostrados en el dashboard.
        Espera una lista de diccionarios con las claves: id, loc, estado.
        """
        with self._lock:
            self._cps = cps.copy()

    # -----------------------------------------------------
    # Crea la aplicación Dash
    # -----------------------------------------------------
    def _crear_app_dash(self):
        app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
        app.title = "EV Central Monitor"

        app.layout = dbc.Container([
            html.H4("*** SD EV CHARGING SOLUTION. MONITOR PANEL ***",
                    className="text-center mt-3 mb-4 text-info"),

            dcc.Interval(id="intervalo", interval=self.intervalo * 1000, n_intervals=0),
            html.Div(id="tabla-cps", className="mt-2"),

            html.Hr(),
            html.Div("Estado del sistema: Central en ejecución",
                     className="text-success text-center mb-2")
        ], fluid=True)

        @app.callback(
            Output("tabla-cps", "children"),
            Input("intervalo", "n_intervals")
        )
        def refrescar_tabla(_):
            with self._lock:
                cps = self._cps.copy()

            filas = []
            for cp in cps:
                color = self.color_estado(cp["estado"])
                filas.append(html.Tr([
                    html.Td(cp["id"]),
                    html.Td(cp["loc"]),
                    html.Td(f"{PRICE_PER_KWH:.2f} €/kWh"),
                    html.Td(cp["estado"],
                            style={
                                "backgroundColor": color,
                                "color": "white",
                                "fontWeight": "bold",
                                "textAlign": "center"
                            })
                ]))

            tabla = dbc.Table([
                html.Thead(html.Tr([
                    html.Th("ID CP"), html.Th("Ubicación"),
                    html.Th("Precio"), html.Th("Estado")
                ])),
                html.Tbody(filas)
            ], bordered=True, striped=True, hover=True, responsive=True)

            return tabla

        self.app = app
        return app

    # -----------------------------------------------------
    # Inicia el servidor Dash y abre Chrome
    # -----------------------------------------------------
    def iniciar(self):
        app = self._crear_app_dash()

        def abrir_navegador():
            time.sleep(1)
            try:
                webbrowser.get("chrome").open_new(f"http://127.0.0.1:{self.puerto}")
            except webbrowser.Error:
                webbrowser.open_new(f"http://127.0.0.1:{self.puerto}")

        threading.Thread(target=abrir_navegador, daemon=True).start()

        app.run(
            host="0.0.0.0",
            port=self.puerto,
            debug=False
        )

