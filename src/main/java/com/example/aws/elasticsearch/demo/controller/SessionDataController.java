package com.example.aws.elasticsearch.demo.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.example.aws.elasticsearch.demo.document.SessionData;
import com.example.aws.elasticsearch.demo.service.SessionDataService;

@RestController()
public class SessionDataController {

	private SessionDataService service;

	@Autowired
	public SessionDataController(SessionDataService service) {
		this.service = service;
	}

	@GetMapping("/test")
	public String test() {
		return "Success";
	}

	@PostMapping("/create")
	public ResponseEntity createSessionData(@RequestBody SessionData sessionData) throws Exception {
		return new ResponseEntity(service.createSessionData(sessionData), HttpStatus.CREATED);
	}
	
	@PostMapping("/createIndex")
	public ResponseEntity createElasticIndex(@RequestBody SessionData sessionData) throws Exception {
		return new ResponseEntity(service.createElasticIndex(sessionData), HttpStatus.CREATED);
	}
	
	

	@GetMapping("/search")
	public List<SessionData> findAllSessionData() throws Exception {
		return service.findAllSessionData();
	}

	@GetMapping("/search/{id}") // -------- not working
	public SessionData findSessionDataById(@PathVariable String id) throws Exception {
		return service.findSessionDataById(id);
	}

	@GetMapping("/search/{name}")
	public List<SessionData> findSessionDataByName(@PathVariable String name) throws Exception {
		return service.findSessionDataByName(name);
	}

	@GetMapping("/search/{fromId}/{toId}/{metaData}")
	public List<SessionData> findSessionDataByNameId(@PathVariable String fromId, @PathVariable String toId,
			@PathVariable String metaData) throws Exception {
		return service.findSessionDataByNameId(fromId, toId, metaData);
	}
	
	@GetMapping("/search/{fromId}/{metaData}")
	public List<SessionData> findSessionDataByFromId(@PathVariable String fromId, @PathVariable String metaData) throws Exception {
		return service.findSessionDataByFromId(fromId, metaData);
	}
	
	@PutMapping("/update")
	public ResponseEntity updateSessionData(@RequestBody SessionData sessionData) throws Exception {
		return new ResponseEntity(service.updateSessionData(sessionData), HttpStatus.CREATED);
	}

	@DeleteMapping("/delete")
	public String deleteSessionData(@PathVariable String id) throws Exception {
		return service.deleteSessionData(id);

	}

}
