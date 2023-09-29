import itertools
import rdflib
from rdflib.namespace import RDF, RDFS, OWL


def extract_data(graph, content_type, max_size):
    """
    Returns the correct function to extract the given data field.

    :param graph: (rdflib.Graph) parsed dataset.
    :param content_type: (str) data field.
    :param max_size: (int) maximum number of items to extract for the given data field.
    """
    if content_type == 'classes':
        return extract_classes(graph, max_size)
    elif content_type == 'properties':
        return extract_properties(graph, max_size)
    elif content_type == 'entities':
        return extract_entities(graph, max_size)
    elif content_type == 'literals':
        return extract_literals(graph, max_size)
    else:
        raise ValueError('content_type must be in [classes, properties, entities, literals]')


def extract_classes(graph, max_size):
    """
    Extracts the classes from the parsed file.

    :param graph: (rdflib.Graph) parsed file.
    :param max_size: (int) maximum number of classes to extract.

    :return (list(str), int) list of classes and the number of classes extracted.
    """
    classes = []
    count = 0
    for class_uri in itertools.chain(graph.subjects(RDF.type, RDFS.Class),
                                     graph.subjects(RDF.type, OWL.Class),
                                     graph.objects(None, RDF.type)):
        # Skip blank nodes
        if not isinstance(class_uri, rdflib.term.BNode):
            if count < max_size:
                classes.append(class_uri.toPython())
            count += 1
    return classes, count


def extract_properties(graph, max_size):
    """
    Extracts the properties from the parsed file.

    :param graph: (rdflib.Graph) parsed file.
    :param max_size: (int) maximum number of properties to extract.

    :return (list(str), int) list of properties and the number of properties extracted.
    """
    properties = []
    count = 0
    for s, p, o in graph:
        if isinstance(p, rdflib.term.URIRef):
            if count < max_size:
                properties.append(p.toPython())
            count += 1
    return properties, count


def extract_entities(graph, max_size):
    """
    Extracts the entities from the parsed file.

    :param graph: (rdflib.Graph) parsed file.
    :param max_size: (int) maximum number of entities to extract.

    :return (list(str), int) list of entities and the number of entities extracted.
    """
    entities = []
    count = 0
    for s, p, o in graph:
        if isinstance(s, rdflib.term.URIRef):
            if count < max_size:
                entities.append(s.toPython())
            count += 1
        if isinstance(o, rdflib.term.URIRef):
            if count < max_size:
                entities.append(o.toPython())
            count += 1
    return entities, count


def extract_literals(graph, max_size):
    """
    Extracts the literals from the parsed file.

    :param graph: (rdflib.Graph) parsed file.
    :param max_size: (int) maximum number of literals to extract.

    :return (list(str), int) list of literals and the number of literals extracted.
    """
    literals = []
    count = 0
    for s, p, o in graph:
        if isinstance(o, rdflib.term.Literal):
            if count < max_size:
                try:
                    literals.append(str(o.toPython()))
                # skip ill-formatted datetime
                except ValueError:
                    continue
            count += 1
    return literals, count
