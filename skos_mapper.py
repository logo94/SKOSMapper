# SKOSMapper.py

# Python packages
import os, io
from typing import Optional, List, Dict, Any, Union

# RDFLib
import rdflib
from rdflib.term import URIRef, Literal, Node
from rdflib.namespace import SKOS, RDF, RDFS, DC, DCTERMS, XSD, OWL
from rdflib.plugins.stores.sparqlstore import SPARQLStore

# Scripts
from dist.models import FieldType, SkosFieldInfo, TriplePattern, TripleQueryParam


# Conditional import of Skosify
try:
    import skosify as skosify_module
except ImportError as e:
    print(e)
    print("WARNING: Skosify library not imported. Skip normalization.")


class SKOSMapper:
    """ Manages the loading, normalization and querying of a SKOS vocabulary """
    
    # ---------------------------------------------------------------------------
    # CLASS SETUP AND CONFIGURATION
    # ---------------------------------------------------------------------------
    
    SKOS_SCHEMA = {
        
        # --- Literal fields ---
        "prefLabel": SkosFieldInfo(name="prefLabel", field_type=FieldType.LITERAL, lang_dependent=True, multivalued=False),
        "altLabel": SkosFieldInfo(name="altLabel", field_type=FieldType.LITERAL, lang_dependent=True),
        "hiddenLabel": SkosFieldInfo(name="hiddenLabel", field_type=FieldType.LITERAL, lang_dependent=True),
        "notation": SkosFieldInfo(name="notation", field_type=FieldType.EITHER, lang_dependent=False),
        "definition": SkosFieldInfo(name="definition", field_type=FieldType.LITERAL, lang_dependent=True),
        
        # --- Relation fields ---
        "broader": SkosFieldInfo(name="broader", field_type=FieldType.URI),
        "narrower": SkosFieldInfo(name="narrower", field_type=FieldType.URI),
        "related": SkosFieldInfo(name="related", field_type=FieldType.URI),
        
        # --- Mapping fields ---
        "exactMatch": SkosFieldInfo(name="exactMatch", field_type=FieldType.URI),
        "closeMatch": SkosFieldInfo(name="closeMatch", field_type=FieldType.URI),
        "broadMatch": SkosFieldInfo(name="broadMatch", field_type=FieldType.URI),
        "narrowMatch": SkosFieldInfo(name="narrowMatch", field_type=FieldType.URI),
        "relatedMatch": SkosFieldInfo(name="relatedMatch", field_type=FieldType.URI)
    }

    def __init__(self, source: str, config_file: str = None, default_lang: str = "en"):
        """
        Initializes the manager by loading, normalizing, and populating the graph

        Args:
            source: Local SKOS file path (e.g., 'sdg.ttl')
                or SPARQL endpoint URL (e.g., 'http://...').
            
            config_file: Path to an INI/CFG format (Skosify standard) configuration file detailing 
                the mappings between non-SKOS source predicates (e.g., GND, RDFS) and the corresponding SKOS fields. 
                This file dictates how data is normalized for the local cache and which fields are dynamically queried on the remote endpoint.
            
            default_lang: default lang code (e.g. 'it', 'en', 'de'). Language can be set in every query class method
        """
        # Query parameters
        self.source = source
        self.config_file = config_file
        self.default_lang = default_lang
        
        # Data classes
        self.TriplePattern = TriplePattern
        self.TripleQueryParam = TripleQueryParam
        
        # Internal data
        self.graph: Optional[rdflib.Graph] = None
        
        # Mappings
        self.field_map: Dict[str, List[str]] = {}
        self.namespaces: Dict[str, str] = {}
        self.uri_map: Dict[URIRef, URIRef] = {}
        
        # Determine if the source is a SPARQL endpoint or a local file
        self.is_sparql_endpoint: bool = self.source.startswith('http')
        
        # Load Skosify configuration if available
        self.skosify_config = skosify_module.config(self.config_file) if 'skosify_module' in globals() else {}
        
        # Load source and configuration
        self._load_source()
        self._load_config()

    # ---------------------------------------------------------------------------
    # PUBLIC QUERY METHODS
    # ---------------------------------------------------------------------------
    
    # Concepts
    
    def get_all_concepts(self, lang: Optional[str] = None, raw_response: bool = False) -> List[Dict[str, Any]]:
        """ Retrieves a list of all concepts in the schema """
        
        if self.is_sparql_endpoint:
            
            pattern = self._sparql_builder(
                s='?concept',
                p=self._field_mapping("Concept"),
                o=SKOS.Concept
            )
            query = f"SELECT DISTINCT ?concept WHERE {{ {pattern} }}"
            results = self._execute_query(query)
            concept_uris = [str(row['concept']) for row in results]
        else:
            pattern = self.TriplePattern(s=None, p=RDF.type, o=SKOS.Concept)
            triples = self._execute_triples(pattern)
            concept_uris = list(triples.get(str(RDF.type), []))

        all_concepts_data = []
        for uri in concept_uris:
            full_data = self._get_full_concept(str(uri), lang=lang, raw_response=raw_response)
            all_concepts_data.append(full_data)
        
        return all_concepts_data
    
    def get_concepts_count(self, lang: Optional[str] = None, raw_response: bool = False) -> int:
        """ Retrieves the number of concepts in the schema """
        
        all_concepts_count = len(self.get_all_concepts(lang, raw_response))
        return all_concepts_count
        
    def get_concept_by_uri(self, uri: str, lang: Optional[str] = None, raw_response: bool = False) -> Optional[Dict[str, Any]]:
        """ Retrieves all triples (predicate/object) for a concept URI """
        
        return self._get_full_concept(uri, lang=lang, raw_response=raw_response)
    
    def get_concept_by_preflabel(self, label: str, lang: str = None, raw_response: bool = False) -> Optional[Dict[str, Any]]:
        """Gets a vocabulary concept by its exact preferred label and language tag."""
        
        uri = self.get_uri_by_prefLabel(label, lang)
        print('uri', uri)
        return self.get_concept_by_uri(uri, lang=lang, raw_response=raw_response) if uri else None
    
    def search_concepts_by_label_regex(self, keyword: str, lang: Optional[str] = None, raw_response: bool = False) -> List[Dict[str, Any]]:
        """Searches concepts by keyword regex on prefLabel and altLabel."""
        
        pref_pred = self._field_mapping("prefLabel")
        alt_pred = self._field_mapping("altLabel")
        all_pred = pref_pred + alt_pred

        pattern = self._sparql_builder(
            s='?concept',
            p=all_pred,
            o='?label',
            lang=lang,
            regex=keyword
        )

        query = f"SELECT DISTINCT ?concept WHERE {{ {pattern} }}"

        results = self._execute_query(query)
        
        
        if self.is_sparql_endpoint:
            concept_uris = [str(row['concept']) for row in results]
            
        else:
            concept_uris = [URIRef(row[0]) for row in results]

        results = []
        for uri in concept_uris:
            full_data = self._get_full_concept(str(uri), lang=lang, raw_response=raw_response)
            results.append(full_data)

        return results
    
    def get_concept_by_notation(self, notation: str, lang: Optional[str] = None, raw_response: bool = False) ->  Optional[Dict[str, Any]]:
        """ Retrieves all fields for a concept based on its skos:notation """
        uri = self.get_uri_by_notation(notation)
        return self.get_concept_by_uri(uri, lang, raw_response) if uri else None
    
    def get_concept_by_skos_relation(self, relation_value: str, relation_type: str, lang: Optional[str] = None, raw_response: bool = False) -> Optional[Dict[str, Any]]:
        """
        Retrieves a concept based on a specific SKOS relation (e.g., broader, narrower, exactMatch, etc.).
        Args:
            relation_value: The value of the relation (e.g., external URI).
            relation_type: The type of SKOS relation (e.g., 'broadMatch', 'exactMatch', etc.).
            lang: Optional language filter for literals.
            raw_response: If True, returns raw RDF nodes instead of normalized strings.
        Returns:
            The full concept data as a dictionary, or None if not found.
        """
        uri = getattr(self, f"get_uri_by_{relation_type}")(relation_value)
        if uri:
            return self.get_concept_by_uri(uri, lang, raw_response)
        return None

    def get_concept_by_broadMatch(self, external_uri: str, lang: Optional[str] = None, raw_response: bool = False) -> Optional[Dict[str, Any]]:
        """
        Retrieves a concept by its exact skos:broadMatch URI.

        Args:
            external_uri: The exact URI of the external concept to match.
            lang: Optional language filter for literals.
            raw_response: If True, returns raw RDF nodes instead of normalized strings.

        Returns:
            The full concept data as a dictionary, or None if not found.
        """
        return self.get_concept_by_skos_relation(external_uri, "broadMatch", lang, raw_response)

    def get_concept_by_narrowMatch(self, external_uri: str, lang: Optional[str] = None, raw_response: bool = False) -> Optional[Dict[str, Any]]:
        """
        Retrieves a concept by its exact skos:narrowMatch URI.

        Args:
            external_uri: The exact URI of the external concept to match.
            lang: Optional language filter for literals.
            raw_response: If True, returns raw RDF nodes instead of normalized strings.

        Returns:
            The full concept data as a dictionary, or None if not found.
        """
        return self.get_concept_by_skos_relation(external_uri, "narrowMatch", lang, raw_response)

    def get_concept_by_relatedMatch(self, external_uri: str, lang: Optional[str] = None, raw_response: bool = False) -> Optional[Dict[str, Any]]:
        """
        Retrieves a concept by its exact skos:relatedMatch URI.

        Args:
            external_uri: The exact URI of the external concept to match.
            lang: Optional language filter for literals.
            raw_response: If True, returns raw RDF nodes instead of normalized strings.

        Returns:
            The full concept data as a dictionary, or None if not found.
        """
        return self.get_concept_by_skos_relation(external_uri, "relatedMatch", lang, raw_response)

    def get_concept_by_exactMatch(self, external_uri: str, lang: Optional[str] = None, raw_response: bool = False) -> Optional[Dict[str, Any]]:
        """
        Retrieves a concept by its exact skos:exactMatch URI.

        Args:
            external_uri: The exact URI of the external concept to match.
            lang: Optional language filter for literals.
            raw_response: If True, returns raw RDF nodes instead of normalized strings.

        Returns:
            The full concept data as a dictionary, or None if not found.
        """
        return self.get_concept_by_skos_relation(external_uri, "exactMatch", lang, raw_response)

    def get_concept_by_closeMatch(self, external_uri: str, lang: Optional[str] = None, raw_response: bool = False) -> Optional[Dict[str, Any]]:
        """
        Retrieves a concept by its exact skos:closeMatch URI.

        Args:
            external_uri: The exact URI of the external concept to match.
            lang: Optional language filter for literals.
            raw_response: If True, returns raw RDF nodes instead of normalized strings.

        Returns:
            The full concept data as a dictionary, or None if not found.
        """
        return self.get_concept_by_skos_relation(external_uri, "closeMatch", lang, raw_response)

    def get_broader_concepts(self, uri: str, lang: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieves the broader concepts for a given concept URI, returning the full concept data.

        Args:
            uri: The URI of the concept for which broader concepts are to be retrieved.
            lang: Optional language filter for literals.

        Returns:
            A list of dictionaries containing the broader concepts' full data.
        """
        return self._get_related_concepts(uri, "broader", lang)

    def get_narrower_concepts(self, uri: str, lang: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieves the narrower concepts for a given concept URI, returning the full concept data.

        Args:
            uri: The URI of the concept for which narrower concepts are to be retrieved.
            lang: Optional language filter for literals.

        Returns:
            A list of dictionaries containing the narrower concepts' full data.
        """
        return self._get_related_concepts(uri, "narrower", lang)

    def get_related_concepts(self, uri: str, lang: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieves related concepts for a given concept URI, returning the full concept data.

        Args:
            uri: The URI of the concept for which related concepts are to be retrieved.
            lang: Optional language filter for literals.

        Returns:
            A list of dictionaries containing the related concepts' full data.
        """
        return self._get_related_concepts(uri, "related", lang)

    def _get_related_concepts(self, uri: str, relation_type: str, lang: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Helper function to retrieve related concepts based on a specific SKOS relation.

        Args:
            uri: The URI of the concept whose related concepts need to be retrieved.
            relation_type: The type of SKOS relation (e.g., 'broader', 'narrower', 'related', etc.).
            lang: Optional language filter for literals.

        Returns:
            A list of dictionaries containing the related concepts' full data.
        """
        related_uris = self.get_related_uris(uri, relation_type)
        return [self._get_full_concept(rel_uri, lang=lang) for rel_uri in related_uris]
    
    # Subjects
    
    def get_uri_by_prefLabel(self, label: str, lang: str = None) -> Optional[str]:
        """
        Retrieve the URI of a SKOS concept based on its exact preferred label.

        Args:
            label: The exact preferred label of the concept.
            lang: Optional language tag for filtering the label.

        Returns:
            The URI of the concept if found, otherwise None.
        """
        params = self.TripleQueryParam(value=label, skos_field='prefLabel', lang=lang)
        result = self._get_field_value(params)
        return result[0] if isinstance(result, list) and result else result
    
    def get_uri_by_altLabel(self, label: str, lang: str = None) -> List[str]:
        """
        Retrieve the URI of a SKOS concept based on its exact alternative label.

        Args:
            label: The exact alternative label of the concept.
            lang: Optional language tag for filtering the label.

        Returns:
            The URI of the concept if found, otherwise None.
        """
        params = self.TripleQueryParam(value=label, skos_field='altLabel', lang=lang)
        result = self._get_field_value(params)
        return result[0] if isinstance(result, list) and result else result
        
    def get_uri_by_notation(self, notation_value: str, lang: Optional[str] = None) -> Optional[str]:
        """
        Retrieve the URI of a SKOS concept based on its skos:notation.

        Args:
            notation_value: The notation string of the concept.

        Returns:
            The URI of the concept if found, otherwise None.
        """   
        
        params = self.TripleQueryParam(value=notation_value, skos_field='notation', lang=None)
        result = self._get_field_value(params)
        return result[0] if isinstance(result, list) and result else result
    
    def get_uri_by_closeMatch(self, external_uri: str) -> Optional[str]:
        """
        Retrieve the URI of a SKOS concept that has a skos:closeMatch to the given external URI.

        Args:
            external_uri: The URI of the external concept.

        Returns:
            The URI of the matching SKOS concept if found, otherwise None.
        """    
        
        params = self.TripleQueryParam(value=external_uri, skos_field='closeMatch')
        result = self._get_field_value(params)
        return result[0] if isinstance(result, list) and result else result
    
    def get_uri_by_exactMatch(self, external_uri: str) -> Optional[str]:
        """
        Retrieve the URI of a SKOS concept that has a skos:exactMatch to the given external URI.

        Args:
            external_uri: The URI of the external concept.

        Returns:
            The URI of the matching SKOS concept if found, otherwise None.
        """   
        
        params = self.TripleQueryParam(value=external_uri, skos_field='exactMatch')
        result = self._get_field_value(params)
        return result[0] if isinstance(result, list) and result else result
    
    def get_uri_by_relatedMatch(self, external_uri: str) -> Optional[str]:
        """
        Retrieve the URI of a SKOS concept that has a skos:relatedMatch to the given external URI.

        Args:
            external_uri: The URI of the external concept.

        Returns:
            The URI of the matching SKOS concept if found, otherwise None.
        """    
        
        params = self.TripleQueryParam(value=external_uri, skos_field='relatedMatch')
        result = self._get_field_value(params)
        return result[0] if isinstance(result, list) and result else result
    
    def get_uri_by_narrowMatch(self, external_uri: str) -> Optional[str]:
        """
        Retrieve the URI of a SKOS concept that has a skos:narrowMatch to the given external URI.

        Args:
            external_uri: The URI of the external concept.

        Returns:
            The URI of the matching SKOS concept if found, otherwise None.
        """    
        
        params = self.TripleQueryParam(value=external_uri, skos_field='narrowMatch')
        result = self._get_field_value(params)
        return result[0] if isinstance(result, list) and result else result
    
    def get_uri_by_broadMatch(self, external_uri: str) -> Optional[str]:
        """
        Retrieve the URI of a SKOS concept that has a skos:broadMatch to the given external URI.

        Args:
            external_uri: The URI of the external concept.

        Returns:
            The URI of the matching SKOS concept if found, otherwise None.
        """   
        
        params = self.TripleQueryParam(value=external_uri, skos_field='broadMatch')
        result = self._get_field_value(params)
        return result[0] if isinstance(result, list) and result else result
    
    def get_uri_by_custom_field(self, field_name: str, value: str, lang: Optional[str] = None) -> Optional[str]:
        """
        Retrieves the URI (?concept) of a SKOS concept based on a custom field.

        Args:
            field_name: The mapped SKOS field name or custom predicate (e.g., 'altLabel', 'myCustomID').
            value: The exact value to match for the given field.
            lang: Optional language tag to filter literal values.

        Returns:
            The URI of the concept as a string, or None if not found.
        """
        
        params = self.TripleQueryParam(value=value, skos_field=field_name, lang=lang)
        result = self._get_field_value(params)
        return result[0] if isinstance(result, list) and result else result

    
    # Predicates
    
    def get_predicates_list(self, uri: str) -> List[str]:
        """
        Returns a list of all predicates associated with the given concept URI

        Args:
            uri: The URI of the concept whose predicates are to be retrieved

        Returns:
            A list of predicate names (as strings) linked to the specified concept
            Returns an empty list if the URI is not found or has no associated predicates
        """
        concept_data = self.get_concept_by_uri(uri, raw_response=True)
        if not concept_data:
            return []
        
        return list(concept_data.keys())
    

    # Objects
    
    def get_prefLabel(self, uri: str, lang: Optional[str] = None) -> Optional[str]:
        """
        Retrieves the preferred label (skos:prefLabel) of a concept.

        Args:
            uri: The URI of the concept.
            lang: Optional language tag to filter the label.

        Returns:
            The preferred label as a string, or None if not found.
        """
        params = self.TripleQueryParam(uri=uri, skos_field='prefLabel', lang=lang)
        result = self._get_field_value(params)
        if isinstance(result, list):
            return result[0] if result else None
        return result

    def get_altLabels(self, uri: str, lang: Optional[str] = None) -> List[str]:
        """
        Retrieves all alternative labels (skos:altLabel) of a concept.

        Args:
            uri: The URI of the concept.
            lang: Optional language tag to filter the labels.

        Returns:
            A list of alternative labels, empty if none found.
        """
        params = self.TripleQueryParam(uri=uri, skos_field='altLabel', lang=lang)
        result = self._get_field_value(params)
        return result if isinstance(result, list) else []

    def get_definitions(self, uri: str, lang: Optional[str] = None) -> List[str]:
        """
        Retrieves all definitions (skos:definition) of a concept.

        Args:
            uri: The URI of the concept.
            lang: Optional language tag to filter the definitions.

        Returns:
            A list of definitions, empty if none found.
        """
        params = self.TripleQueryParam(uri=uri, skos_field='definition', lang=lang)
        result = self._get_field_value(params)
        return result if isinstance(result, list) else []

    def get_examples(self, uri: str, lang: Optional[str] = None) -> List[str]:
        """
        Retrieves all examples (skos:example) of a concept.

        Args:
            uri: The URI of the concept.
            lang: Optional language tag to filter the examples.

        Returns:
            A list of examples, empty if none found.
        """
        params = self.TripleQueryParam(uri=uri, skos_field='example', lang=lang)
        result = self._get_field_value(params)
        return result if isinstance(result, list) else []

    def get_notation(self, uri: str) -> Optional[str]:
        """
        Retrieves the notation (skos:notation) of a concept.

        Args:
            uri: The URI of the concept.

        Returns:
            The notation as a string, or None if not found.
        """
        params = self.TripleQueryParam(uri=uri, skos_field='notation')
        result = self._get_field_value(params)
        if isinstance(result, list):
            return result
        elif isinstance(result, str):
            return [result]
        else:
            return []

    def get_closeMatch(self, uri: str) -> List[str]:
        """ Retrieves URIs of concepts linked via skos:closeMatch. """
        params = self.TripleQueryParam(uri=uri, skos_field='closeMatch')
        return self._get_field_value(params)

    def get_broadMatch(self, uri: str) -> List[str]:
        """ Retrieves URIs of concepts linked via skos:broadMatch. """
        params = self.TripleQueryParam(uri=uri, skos_field='broadMatch')
        return self._get_field_value(params)

    def get_exactMatch(self, uri: str) -> List[str]:
        """ Retrieves URIs of concepts linked via skos:exactMatch. """
        params = self.TripleQueryParam(uri=uri, skos_field='exactMatch')
        return self._get_field_value(params)

    def get_relatedMatch(self, uri: str) -> List[str]:
        """ Retrieves URIs of concepts linked via skos:relatedMatch. """
        params = self.TripleQueryParam(uri=uri, skos_field='relatedMatch')
        return self._get_field_value(params)

    def get_narrowMatch(self, uri: str) -> List[str]:
        """ Retrieves URIs of concepts linked via skos:narrowMatch. """
        params = self.TripleQueryParam(uri=uri, skos_field='narrowMatch')
        return self._get_field_value(params)

    # Utility getters
    
    def get_label_by_notation(self, notation_value: str, lang: Optional[str] = None) -> Optional[str]:
        """
        Retrieves the preferred label (skos:prefLabel) of a concept given its notation and optional language.

        Args:
            notation_value: The notation value of the concept.
            lang: Optional language tag to filter the prefLabel.

        Returns:
            The preferred label string if found, otherwise None.
        """
        uri = self.get_uri_by_notation(notation_value, lang)
        if not uri:
            return None

        params = self.TripleQueryParam(
            uri=uri,
            skos_field='prefLabel',
            lang=lang
        )
        result = self._get_field_value(params)
        if isinstance(result, list):
            return result[0] if result else None
        return result

    def get_notation_by_label(self, label: str, lang: str = "") -> List[str]:
        """
        Retrieves all skos:notation values for a concept given its exact preferred label and optional language.

        Notes:
            - Even though SKOS recommends one notation per concept per notation system,
            multiple notations may exist (e.g., Dewey Decimal, custom schemes).
            - This function always returns a list for consistency.

        Args:
            label: The exact preferred label of the concept.
            lang: Optional language tag to filter the label.

        Returns:
            A list of notation strings. Returns an empty list if no notation is found.
        """
        uri = self.get_uri_by_prefLabel(label, lang)
        if not uri:
            return []

        return self.get_notation(uri)

    def get_related_uris(self, uri: str, skos_relation_field: str) -> List[str]:
        """
        Helper method to retrieve only the URIs of related concepts via a mapped SKOS relation field.

        Args:
            uri: The URI of the concept.
            skos_relation_field: The SKOS relation field (e.g., 'broader', 'narrower', 'related', 'closeMatch').

        Returns:
            A list of URIs for the related concepts. Returns an empty list if none are found.
        """
        params = self.TripleQueryParam(
            uri=uri,
            skos_field=skos_relation_field
        )
        result = self._get_field_value(params)
        if isinstance(result, list):
            return result
        elif isinstance(result, str):
            return [result]
        return []
    
    # ---------------------------------------------------------------------------
    # LOAD SOURCE / CONFIG
    # ---------------------------------------------------------------------------

    def _load_source(self) -> None:
        """ 
        Loads the source vocabulary
        
        - If the source is a local file:
            * If a previously SKOSified cached version exists, it is loaded
            * Otherwise, the source file is SKOSified and the result is stored in the cache (.skos.ttl)
        - If the source is a remote SPARQL endpoint:
            * No graph is loaded locally and all queries will be executed remotely
        """
        
        # Remote endpoint
        if self.is_sparql_endpoint:
            print(f"Querying SPARQL Endpoint: {self.source}")
            self.graph = None
            return
        
        # Local file
        source_dir = os.path.dirname(self.source)
        source_name = os.path.basename(self.source)
        
        if source_name.endswith('.skos.ttl'):
            normalized_path = self.source
        else:
            normalized_name = source_name.replace(os.path.splitext(source_name)[-1], '.skos.ttl')
            normalized_path = os.path.join(source_dir, normalized_name)
        
        # Load cached SKOSified version if present
        if os.path.exists(normalized_path):
            print(f"SKOS cache found: loading graph from {normalized_path}...")
            
            try:
                self.graph = rdflib.Graph()
                self.graph.parse(normalized_path)
                print(f"Loaded {len(self.graph)} SKOS triples from cache file")
                return
            except Exception as e:
                print(f"ERROR while loading the cache. Proceeding with normalization: {e}")
        
        # If no cache exists, the source file MUST exist
        if not os.path.exists(self.source):
            print(f"ERROR: Neither source file '{self.source}' nor cache '{normalized_path}' exist")
            return

        # Normalize the source file with Skosify
        print(f"SKOS cache not found. Normalizing source file: {self.source}...")
        try:
            raw_graph = rdflib.Graph()
            raw_graph.parse(self.source)

            # Skosify
            skos_graph = skosify_module.skosify(raw_graph, **self.skosify_config)
            skos_graph.serialize(destination=normalized_path, format='turtle')

            self.graph = skos_graph
            print(f"Normalization completed. {len(self.graph)} triples saved to cache file")

        except Exception as e:
            print(f"ERROR during Skosify normalization: {e}")
            self.graph = None
    
    def _load_config(self) -> None:
        """
        Loads the normalization and mapping configuration used to interpret SKOS fields

        The configuration defines:
        - Custom namespaces
        - Literal fields and relation fields that map external predicates to SKOS fields
        - Concept fields (mapping subject types to SKOS:Concept)
        - Default SKOS mappings for fields not explicitly overridden in the configuration

        The result is stored in:
        - self.namespaces  : prefix → namespace URI
        - self.field_map   : SKOS field name → list of predicate URIs
        """
        
        # Namespaces
        self.namespaces = {
            'skos': str(SKOS),
            'rdf': str(RDF),
            'rdfs': str(RDFS),
            'owl': str(OWL),
            'dc': str(DC),
            'dcterms': str(DCTERMS),
            'xsd': str(XSD),
        }
        
        custom_namespaces = self.skosify_config.get('namespaces', {})
        self.namespaces.update(custom_namespaces)
        
        # Collect field mappings: literal_fields, relation_fields, concept_fields
        self.field_map: Dict[str, List[str]] = {}
        
        all_mappings = []
        all_mappings.extend(self.skosify_config.get('literal_fields', []))
        all_mappings.extend(self.skosify_config.get('relation_fields', []))
        
        # Concept mapping (SKOS Concept only)
        for src, tgt in self.skosify_config.get('concept_fields', []):
            if tgt.lower() == 'skos:concept':
                all_mappings.append((src, 'skos:Concept'))

        # Add field to field_map
        for src_qname_like, target_skos_qname in all_mappings:
            
            # Normalize the SKOS field name ("skos:prefLabel" → "prefLabel")
            key = target_skos_qname.split(':')[-1]
            
            src_uri = self.expand_qname(src_qname_like)
            
            if not src_uri:
                print(f"WARNING: Cannot expand predicate '{src_qname_like}' (no namespace or invalid). Skipping.")
                continue
            
            self.field_map.setdefault(key, [])
            
            if src_uri not in self.field_map[key]:
                self.field_map[key].append(src_uri)
        
        # Map all standard SKOS fields
        for field in self.SKOS_SCHEMA.keys():
            if field not in self.field_map:
                skos_uri = self.expand_qname(f"skos:{field}")
                if skos_uri:
                    self.field_map[field] = [skos_uri]

        # Ensure 'Concept' is always present to traslate non-SKOS items into skos:Concept
        if 'Concept' not in self.field_map:
            self.field_map['Concept'] = [SKOS.Concept]

    # ---------------------------------------------------------------------------
    # UTILS
    # ---------------------------------------------------------------------------
    
    def expand_qname(self, qname: str) -> Optional[URIRef]:
        """
        Resolves a QName, CURIE-like value or SKOS field name into a URIRef

        Supported input formats:
        - Standard QName:          
            "skos:prefLabel"  →  URIRef("http://www.w3.org/2004/02/skos/core#prefLabel")
        - Dot-notation (auto-converted to prefix:name):
            "gndo.label"      →  URIRef("<namespace_of_gndo>label")
        - Bare SKOS attribute:
            "prefLabel"       →  URIRef(SKOS.prefLabel)
        - Special SKOS/RDF tokens:
            "Concept"         →  URIRef(SKOS.Concept)
            "type"            →  URIRef(RDF.type)
        - Special null token:
            "none"            →  None

        Returns:
            URIRef or None if it cannot be resolved
        """
        
        # Input check
        if not isinstance(qname, str):
            return None
        
        name = qname.strip()
        
        # Full URI
        if "://" in name:
            return URIRef(name)

        # Special tokens
        if name.lower() == "none":
            return None
        if name == "type":
            return RDF.type
        if name == "Concept":
            return SKOS.Concept
        
        # Convert dot-notation → prefix:name
        if "." in name and ":" not in name:
            prefix, local = name.split(".", 1)
            name = f"{prefix}:{local}"
        
        # Try SKOS.<field> if no prefix provided
        if ":" not in name:
            try:
                return getattr(SKOS, name)
            except AttributeError:
                pass
            
        # Resolve prefix:name using configured namespaces
        if ":" in name:
            prefix, local = name.split(":", 1)
            if prefix in self.namespaces:
                return URIRef(self.namespaces[prefix] + local)
            
        return None
    
    def _field_mapping(self, skos_field: str) -> List[URIRef]:
        """
        Always returns a list of expanded URIRefs for the given SKOS field.

        - If the field is not mapped, returns an empty list.
        - Expands QNames or special names using `expand_qname`.
        - Filters out any URIs that could not be resolved (None).
        """
        
        mapped = self.field_map.get(skos_field, [])
        
        if not mapped:
            print(f"WARNING: Field '{skos_field}' not found in field map.")
            return []
        
        expanded = []
        for item in mapped:
            
            if isinstance(item, URIRef):
                expanded.append(item)
                continue
            
            if isinstance(item, str):
                uri = self.expand_qname(item)
                if uri:
                    expanded.append(uri)
                else:
                    print(f"WARNING: Could not expand predicate '{item}' for field '{skos_field}'.")
                    
        if not expanded:
            print(f"WARNING: None of the mapped URIs for field '{skos_field}' could be expanded.")
            
        return expanded
    
    def _sparql_builder(self, s: str, p: Union[List[URIRef], URIRef], o: str, lang: Optional[str] = None, label: Optional[str] = None, regex: Optional[str] = None) -> str:
        """
        Builds SPARQL triple patterns with optional UNION (for multiple predicates),
        language filter, and regex filter.

        Args:
            s: Subject variable or URI string
            p: Single URIRef or list of URIRef predicates
            o: Object variable or value
            lang: Optional language filter (for literals)
            regex: Optional regex filter (applied to object as string)

        Returns:
            SPARQL string
        """
        if not isinstance(p, list):
            p = [p]

        patterns = []
        for pred in p:
            
            s_fmt = s
            p_fmt = f"<{pred}>" if isinstance(pred, URIRef) else str(pred)
            o_fmt = o
            clause = f"{{ {s_fmt} {p_fmt} {o_fmt} }}"
            filters = []
            
            if regex is not None:
                filters.append(f'FILTER regex(str({o_fmt}), "{regex}", "i")')
            
            if label is not None:
                filters.append(f'FILTER(STR({o_fmt}) = "{label}")')
            
            if lang is not None:
                filters.append(f'FILTER (lang({o_fmt}) = "{lang}")')
            
            if filters:
                clause += " " + " ".join(filters)
            patterns.append(clause)

        return " UNION ".join(patterns)
    
    # ---------------------------------------------------------------------------
    # PRIVATE QUERY METHODS
    # ---------------------------------------------------------------------------
    
    def _get_full_concept(self, uri: str, lang: Optional[str] = None, raw_response: bool = False) -> Dict[str, List[Any]]:
        target_uri = URIRef(uri)
        raw_data = {}

        # Remote
        if self.is_sparql_endpoint:
            pattern = self._sparql_builder(uri, '?p', 'o')
            query = f"SELECT DISTINCT ?p ?o WHERE {{ {pattern} }}"
            results = self._execute_query(query)

            for row in results:
                p_uri = str(row['p'])
                raw_data.setdefault(p_uri, []).append(row['o'])
        
        # Local
        else:
            pattern = self.TriplePattern(s=target_uri, p=None, o=None)
            raw_data = self._execute_triples(pattern)

        if raw_response:
            return raw_data

        return self._normalize_raw_data(raw_data, lang or self.default_lang)
    
    def _get_field_value(self, params: TripleQueryParam):
        """
        Unified triple resolver (local + SPARQL).

        Handles:
        - forward triple lookup (URI → predicate → values)
        - reverse lookup (value → predicate → URI)
        - literal reverse lookup with language filtering
        """
        
        skos_field = params.skos_field
        target_lang = params.lang or self.default_lang
        
        field_info = self.SKOS_SCHEMA.get(skos_field)
        if not field_info:
            raise ValueError(f"Unknown SKOS field: {skos_field}")
        
        is_literal = field_info.field_type == FieldType.LITERAL
        is_uri_list = field_info.field_type == FieldType.URI
        is_either = field_info.field_type == FieldType.EITHER

        predicates = self._field_mapping(skos_field)
        
        if params.value is not None and (is_literal or is_either):
            mode = "reverse_literal" # Literal -> URI
        elif params.uri is not None:
            mode = "forward"    # URI -> values
        elif params.value is not None and is_uri_list:
            mode = "reverse_uri"    # URI -> URI
        else:
            return []
        
        # URI -> values
        if mode == "forward":
            
            if self.is_sparql_endpoint:
                pattern = self._sparql_builder(
                    s=f"<{params.uri}>",
                    p=predicates,
                    o='?value'
                )
                query = f"SELECT DISTINCT ?value WHERE {{ {pattern} }}"
                rows = self._execute_query(query)
                raw_nodes = [row['value'] for row in rows]
            else:
                collected = []
                for pred in predicates:
                    pattern = self.TriplePattern(s=URIRef(params.uri), p=pred, o=None)
                    triples = self._execute_triples(pattern)
                    collected.extend(triples.get(str(pred), []))
                raw_nodes = collected

        # # URI -> URI
        elif mode == "reverse_uri":
    
            if self.is_sparql_endpoint:
                
                pattern = self._sparql_builder(
                    s='?subject',
                    p=predicates,
                    o=f"<{params.value}>"
                )
                query = f"SELECT DISTINCT ?subject WHERE {{ {pattern} }}"
                rows = self._execute_query(query)
                raw_nodes = [row['subject'] for row in rows]
                
            else:
                collected = []
                obj_uri = URIRef(params.value)
                for pred in predicates:
                    for s, _, _ in self.graph.triples((None, pred, obj_uri)):
                        collected.append(s)
                raw_nodes = collected
        
        # Literal -> URI
        elif mode == "reverse_literal":
            
            pattern = self._sparql_builder(
                s='?subject',
                p=predicates,
                o='?literal',
                lang=target_lang,
                label=params.value.strip()
            )
            query = f"""
            SELECT DISTINCT ?subject WHERE {{ {pattern} }}
            """
            rows = self._execute_query(query)
            
            if self.is_sparql_endpoint:
                raw_nodes = [row['subject'] for row in rows]
            else:
                raw_nodes = [URIRef(row[0]) for row in rows]

        # Process results
        return self._extract_values_from_nodes(raw_nodes, skos_field, target_lang)
    
    def _execute_query(self, sparql_query: str) -> Any:
        """  Executes a SPARQL query and returns the raw rdflib result object """
        
        # Remote
        if self.is_sparql_endpoint:
            store = SPARQLStore(query_endpoint=self.source)
            g = rdflib.Graph(store=store)
            try:
                results = g.query(sparql_query)
            except Exception as e:
                print(f"ERROR: Query failed on remote SPARQL endpoint: {e}")
                return []
        
        # Local
        else:
            if self.graph is None or len(self.graph) == 0:
                print("WARNING: Local graph is empty.")
                return []
            results = self.graph.query(sparql_query)
        
        return results
    
    def _execute_triples(self, pattern: "TriplePattern") -> dict[str, list[Node]]:
        """
        Executes a triple pattern on the local RDF graph.

        This helper retrieves all triples matching the given pattern
        (subject, predicate(s), object) and organizes them by predicate.

        Args:
            pattern: A TriplePattern object with optional s, p, o attributes.
                    - s: subject URI or None (wildcard)
                    - p: single predicate URI, list of predicates, or None (wildcard)
                    - o: object URI or None (wildcard)

        Returns:
            A dictionary mapping predicate URIs (as strings) to lists of objects.
            Example:
                {
                    "http://example.org/predicate1": [obj1, obj2],
                    "http://example.org/predicate2": [obj3]
                }
        """
        if self.graph is None:
            return {}

        # Prepare the output dictionary
        results: dict[str, list[Node]] = {}

        # Extract pattern components
        subject = pattern.s if pattern.s else None
        object_ = pattern.o if pattern.o else None

        # Ensure predicates is a list
        if pattern.p is None:
            predicates = [None]
        elif isinstance(pattern.p, list):
            predicates = pattern.p
        else:
            predicates = [pattern.p]

        # Iterate over all matching triples
        for predicate in predicates:
            for s, p, o in self.graph.triples((subject, predicate, object_)):
                pred_str = str(p)
                if pred_str not in results:
                    results[pred_str] = []
                results[pred_str].append(o)

        return results
    
    # ---------------------------------------------------------------------------
    # RESULT MAPPING METHODS
    # ---------------------------------------------------------------------------
    
    def _extract_values_from_nodes(self, nodes: List[Node], skos_field: str, target_lang: str) -> Union[Optional[str], List[str]]:
        """
        Esegue la selezione, il filtro linguistico e la formattazione finale 
        su una lista di nodi rdflib, indipendentemente dalla sorgente.
        """
        if not nodes:
            return None
        
        field_info = self.SKOS_SCHEMA.get(skos_field)
        if not field_info:
            raise ValueError(f"Unknown SKOS field: {skos_field}")
        
        is_uri_field = field_info.field_type == FieldType.URI
        is_literal_field = field_info.field_type == FieldType.LITERAL
        is_either_field = field_info.field_type == FieldType.EITHER
        is_notation_field = skos_field == 'notation'
        
        # URI Fields (Broader, Narrower, Related, Matches)
        if is_uri_field:
            return list({str(node) for node in nodes if isinstance(node, URIRef)})

        # Literal Fields (Labels, Note, Notation)
        best_match = None
        literal_list = []
        
        for node in nodes:
            
            if isinstance(node, URIRef):
                return str(node)
                
            elif isinstance(node, Literal):
                accepted = False
                node_lang = node.language or ""
                
                if is_notation_field:
                    if node.datatype and node.datatype == URIRef("http://dewey.info"):
                        accepted = True
                    elif not node.datatype:
                        accepted = True
                        
                if not node_lang:
                    accepted = True
                
                elif node_lang == target_lang or node_lang == "":
                    accepted = True
                
                
                if accepted:
                    value = str(node).strip()
                    
                    if is_either_field or is_literal_field:
                        literal_list.append(value)
                    else:
                        if node_lang == target_lang:
                            return value
                        
                        if best_match is None:
                            best_match = value

        if is_either_field or is_literal_field:
            return list(set(literal_list))
        
        return best_match
    
    def _normalize_raw_data(self, raw_data: Dict[str, List[Node]], target_lang: str) -> Dict[str, List[str]]:
        """
        Normalizes raw RDF nodes into a dict of field_name -> list of string values.
        
        Args:
            raw_data: Dict[predicate_uri, List[Node]] from SPARQL or local graph
            target_lang: language code for filtering literals
            
        Returns:
            Dict[field_name, List[str]] with normalized string values
        """
        normalized = {}

        for skos_field, mapped_uris in self.field_map.items():
            all_nodes = []
            for uri in mapped_uris:
                uri_str = str(uri)
                if uri_str in raw_data:
                    all_nodes.extend(raw_data[uri_str])

            if all_nodes:
                values = self._extract_values_from_nodes(all_nodes, skos_field, target_lang)
                if values is not None:
                    if isinstance(values, list):
                        normalized[skos_field] = list(set(values))
                    else:
                        normalized[skos_field] = [values]

        # Include any leftover predicates not mapped to SKOS fields
        for predicate_uri, objects in raw_data.items():
            is_mapped = any(str(uri) == predicate_uri for uris in self.field_map.values() for uri in uris)
            if not is_mapped:
                normalized[predicate_uri] = [str(obj) for obj in objects]

        return normalized
    