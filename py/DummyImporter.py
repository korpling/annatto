from graphannis.graph import GraphUpdate


def start_import():
    u = GraphUpdate()
    u.add_node("corpus", "corpus")

    u.add_node("corpus/doc", node_type="corpus")
    u.add_edge("corpus/doc", "corpus", layer="annis", component_name="", component_type="PartOf")

    # Add token example text
    u.add_node("t1")
    u.add_node_label("t1", "annis", "tok", "This")
    u.add_node_label("t1", "annis", "tok-whitespace-after", " ")
    u.add_edge("t1", "corpus/doc", "annis", "PartOf", "")

    u.add_node("t2")
    u.add_node_label("t2", "annis", "tok", "is")
    u.add_node_label("t2", "annis", "tok-whitespace-after", " ")
    u.add_edge("t2", "corpus/doc", "annis", "PartOf", "")

    u.add_node("t3")
    u.add_node_label("t3", "annis", "tok", "an")
    u.add_node_label("#t3", "annis", "tok-whitespace-after", " ")
    u.add_edge("t3", "corpus/doc", "annis", "PartOf", "")

    u.add_node("t4")
    u.add_node_label("t4", "annis", "tok", "example")
    u.add_node_label("t4", "annis", "tok-whitespace-after", " ")
    u.add_edge("t4", "corpus/doc", "annis", "PartOf", "")

    u.add_node("t5")
    u.add_node_label("t5", "annis", "tok", ".")
    u.add_node_label("t5", "annis", "tok-whitespace-after", " ")
    u.add_edge("t5", "corpus/doc", "annis", "PartOf", "")

    # Add Ordering edges
    u.add_edge("t1", "t2", "annis", "Ordering", "")
    u.add_edge("t2", "t3", "annis", "Ordering", "")
    u.add_edge("t3", "t4", "annis", "Ordering", "")
    u.add_edge("t4", "t5", "annis", "Ordering", "") 
    
    return u
