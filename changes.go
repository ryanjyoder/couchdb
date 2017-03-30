package couchdb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/google/go-querystring/query"
)

// ChangesService is an interface for manages the _changes endpoint.
//   http://docs.couchdb.org/en/2.0.0/api/database/changes.html
type ChangesService interface {
	Stream(params ChangesQueryParameters) (<-chan Change, error)
	Poll(params ChangesQueryParameters) (*ChangesResponse, error)
}

// Changes performs actions and certain view documents
type Changes struct {
	Database *Database
}

// ChangesResponse is response for polling the _changes endpoint.
//
// Response JSON Object:
//     last_seq (json) – Last change update sequence
//     pending (number) – Count of remaining items in the feed
//     results (array) – Changes made to a database
//     		JSON Object:
//     			changes (array) – List of document`s leafs with single field rev
//     			id (string) – Document ID
//     			seq (json) – Update sequence
//
// Status Codes:
//     200 OK – Request completed successfully
//     400 Bad Request – Bad request
type ChangesResponse struct {
	LastSeq string   `json:"last_seq,omitempty"`
	Results []Change `json:"results,omitempty"`
	Pending int      `json:"pending,omitempty"`
}

// Change is single row inside results
type Change struct {
	Changes []Rev  `json:"changes"`
	ID      string `json:"id"`
	Seq     string `json:"seq,omitempty"`
}

// Rev hold the rev of the document changed.
type Rev struct {
	Rev string `json:"rev,omitempty"`
}

// ChangesQueryParameters is struct to define url query parameters for the _changes endpoint.
type ChangesQueryParameters struct {
	DocIDs          []string `url:"doc_ids,omitempty"`
	Conflicts       *bool    `url:"conflicts,omitempty"`
	Descending      *bool    `url:"descending,omitempty"`
	Feed            *string  `url:"feed,omitempty"`
	Filter          *string  `url:"filter,omitempty"`
	Heartbeat       *int     `url:"heartbeat,omitempty"`
	IncludeDocs     *bool    `url:"include_docs,omitempty"`
	Attachments     *bool    `url:"attachments,omitempty"`
	AttEncodingInfo *bool    `url:"att_encoding_info,omitempty"`
	LastEventID     *int     `url:"last-event-id,omitempty"`
	Limit           *int     `url:"limit,omitempty"`
	Since           *string  `url:"since,omitempty"`
	Style           *string  `url:"style,omitempty"`
	Timeout         *int     `url:"timeout,omitempty"`
	View            *string  `url:"view,omitempty"`
}

// Stream reads continuously from a changes stream
func (c *Changes) Stream(params ChangesQueryParameters) (<-chan Change, error) {
	continuous := "continuous"
	params.Feed = &continuous // Must be continuous for streaming
	q, err := query.Values(params)
	if err != nil {
		return nil, err
	}
	uri := fmt.Sprintf("%s/_changes?%s", url.PathEscape(c.Database.Name), q.Encode())
	res, err := c.Database.Client.Request(http.MethodGet, uri, nil, "")

	resultsChan := make(chan Change)
	go readStream(res, resultsChan)

	return resultsChan, nil
}
func readStream(r *http.Response, results chan Change) {
	defer r.Body.Close()
	defer close(results)

	reader := bufio.NewReader(r.Body)
	for line, err := reader.ReadBytes('\n'); err == nil; line, err = reader.ReadBytes('\n') {
		line = bytes.TrimSpace(line)

		if len(line) <= 0 {
			continue
		}

		var result Change
		err = json.Unmarshal(line, &result)
		if err == nil && result.Seq != "" {
			results <- result
		}
	}
}

// Poll requests from the database
// This could be a long poll, where the Database blocks until there is new change
func (c *Changes) Poll(params ChangesQueryParameters) (*ChangesResponse, error) {
	// if Feed is set to continuous the server will stream instead of poll, unset it.
	if params.Feed != nil && *(params.Feed) == "continuous" {
		params.Feed = nil
	}
	q, err := query.Values(params)
	if err != nil {
		return nil, err
	}
	uri := fmt.Sprintf("%s/_changes?%s", url.PathEscape(c.Database.Name), q.Encode())
	r, err := c.Database.Client.Request(http.MethodGet, uri, nil, "")
	defer r.Body.Close()

	response := ChangesResponse{}
	err = json.NewDecoder(r.Body).Decode(&response)
	return &response, err
}
