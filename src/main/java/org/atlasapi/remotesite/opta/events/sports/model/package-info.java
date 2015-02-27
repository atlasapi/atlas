/**
 * This model contains a set of classes designed to wrap the Opta feed
 * model as closely as possible, to allow for deserialization using gson's
 * default deserialization mode wherever possible.
 * <p>
 * Not all fields in all classes are used in the parsed events, but are modelled
 * here for completeness and for the avoidance of errors when deserializing.
 */
package org.atlasapi.remotesite.opta.events.sports.model;