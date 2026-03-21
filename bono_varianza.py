from mrjob.job import MRJob

class VarianzaActas(MRJob):

    def mapper(self, _, linea):
        try:
            partes = linea.strip().split(',')

            if len(partes) != 3:
                return

            candidato = partes[0]
            votos = float(partes[2])

            yield candidato, (votos, votos**2, 1)

        except:
            pass

    def reducer(self, candidato, tuplas):
        n = 0
        suma_x = 0
        suma_x2 = 0

        for x, x2, conteo in tuplas:
            n += conteo
            suma_x += x
            suma_x2 += x2

        # INICIA TU CODIGO AQUI
        if n > 0:
            varianza = (suma_x2 / n) - (suma_x / n)**2
            yield candidato, round(varianza, 2)
        # TERMINA TU CODIGO AQUI

if __name__ == '__main__':
    VarianzaActas.run()
