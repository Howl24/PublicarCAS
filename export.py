from cassandra.cluster import Cluster

CAS_KEYSPACE = 'cas'
UNPROCESSED_OFFERS_TABLE = 'unprocessed_offers'

def connectToDatabase(keyspace):
    cluster = Cluster()
    session = cluster.connect(keyspace)
    return session


def getAllOffers(session, table):
    cmd = """
          SELECT * FROM  {0};
          """.format(table)

    try:
       result = session.execute(cmd)
    except:
        print("Ocurrió un fallo al obtener las ofertas")
        result = None

    return result


def removeWhitespaces(text):

    return ' '.join(text.split())


def main():
    session = connectToDatabase(CAS_KEYSPACE)
    offers = getAllOffers(session, UNPROCESSED_OFFERS_TABLE)
    for offer in offers:
        #print(offer)
        desc = offer.features['descripción']
        desc = desc.replace("\t", " ")
        desc = desc.replace("  ", " ")
        desc = desc.replace("  ", " ")
        desc = desc.replace("  ", " ")
        print(desc)

        print("--------------------------------------", end="\n\n")



if __name__ == "__main__":
    main()

