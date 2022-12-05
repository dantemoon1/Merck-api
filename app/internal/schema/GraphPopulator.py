from neo4j import GraphDatabase


class GraphPopulator:

    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    
    def stringify_props(self, props):
        if props == {}:
            return "{}"
        string_props = "{"
        for k,v in props.items():
            if isinstance(v, str):
                string_props += k + ": '" + v + "'" + ", "
            else:
                string_props += k + ": " + str(v) + ", "
        string_props = string_props[:-2]
        string_props += "}"
        return string_props

    def execute(self, **params):
        with self.driver.session() as session:
            if params.get("operation") == "create_node":
                node_label = params.get("node_label")
                node_props = params.get("node_props")
                session.execute_write(self.create_node, node_label, node_props)
            elif params.get("operation") == "create_rel":
                node1_label = params.get("node1_label")
                node1_props = params.get("node1_props")
                node2_label = params.get("node2_label")
                node2_props = params.get("node2_props")
                rel_name = params.get("rel_name")
                rel_props = params.get("rel_props")
                session.execute_write(self.create_rel, node1_label, node1_props, node2_label, node2_props, rel_name, rel_props)
            elif params.get("operation") == "wipe":
                session.execute_write(self.wipe)

    def create_node(self, tx, node_label, node_props):
        node_props_string = self.stringify_props(node_props)
        result = tx.run(
            f"MATCH (p:{node_label} {node_props_string})"
            "RETURN COUNT (p) as cnt"
        )
        if result.single()["cnt"] == 0:
            tx.run(
                f"CREATE (a:{node_label} {node_props_string})"
            )
            print("successfully created node")
        else:
            print("node already exists")

    def create_rel(self, tx, node1_label, node1_props, node2_label, node2_props, rel_name, rel_props):
        node1_props_string = self.stringify_props(node1_props)
        node2_props_string = self.stringify_props(node2_props)
        rel_props_string = self.stringify_props(rel_props)
        result = tx.run(
            f"MATCH (a:{node1_label} {node1_props_string})"
            f"MATCH (b:{node2_label} {node2_props_string})"
            f"MATCH (a)-[r:{rel_name} {rel_props_string}]->(b)"
            "RETURN COUNT (r) as cnt"
        )
        if result.single()["cnt"] == 0:
            tx.run(
                f"MATCH (a:{node1_label} {node1_props_string})"
                f"MATCH (b:{node2_label} {node2_props_string})"
                f"CREATE (a)-[r:{rel_name} {rel_props_string}]->(b)"
            )
            print("successfully created relationship")
        else:
            print("relationship already exists")

    def wipe(self, tx):
        tx.run(
            "MATCH (n) "
            "DETACH DELETE n"
        )
        print("successfully wiped database")

    def close(self):
        self.driver.close()
