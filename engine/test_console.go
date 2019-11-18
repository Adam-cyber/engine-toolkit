package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	"github.com/pkg/errors"
)

// runTestConsole runs the test console that allows developers
// to perform tests against their engine.
// Context is ignored, the console stops when the process/container stops.
func (e *Engine) runTestConsole(context.Context) {
	e.logDebug("running test console...")
	fmt.Println(`
	The Engine Toolkit Test Console is now running.

	Go to: http://localhost:9090/`)
	processWebhookProxy := reverseProxy(os.Getenv("VERITONE_WEBHOOK_PROCESS"))
	readyWebhookProxy := reverseProxy(os.Getenv("VERITONE_WEBHOOK_READY"))
	handleManifest := e.handleManifest("/var/manifest.json")
	if err := http.ListenAndServe("0.0.0.0:9090", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			if _, err := io.WriteString(w, consoleHTML); err != nil {
				log.Println(err)
			}
		case "/api/engine/process":
			processWebhookProxy.ServeHTTP(w, r)
		case "/api/engine/ready":
			readyWebhookProxy.ServeHTTP(w, r)
		case "/api/engine/manifest.json":
			handleManifest(w, r)
		case "/api/engine/env-vars":
			e.handleEnvVars(w, r)
		default:
			http.NotFound(w, r)
		}
	})); err != nil {
		panic(err)
	}
}

func (e *Engine) handleManifest(file string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f, err := os.Open(file)
		if err != nil {
			if os.IsNotExist(err) {
				http.Error(w, "Missing manifest.json", http.StatusInternalServerError)
				return
			}
			http.Error(w, "Could not open manifest.json", http.StatusInternalServerError)
			return
		}
		defer f.Close()
		var m map[string]interface{}
		err = json.NewDecoder(f).Decode(&m)
		if err != nil {
			http.Error(w, "Could not read manifest.json: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if m["engineMode"] != "chunk" {
			http.Error(w, fmt.Sprintf("manifest.json: engineMode: should be \"chunk\" not %q", m["engineMode"]), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(m)
		if err != nil {
			http.Error(w, "Could not encode manifest.json: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func (e *Engine) handleEnvVars(w http.ResponseWriter, r *http.Request) {
	if err := validURL(e.Config.Webhooks.Process.URL); err != nil {
		http.Error(w, "VERITONE_WEBHOOK_PROCESS: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := validURL(e.Config.Webhooks.Ready.URL); err != nil {
		http.Error(w, "VERITONE_WEBHOOK_READY: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func validURL(u string) error {
	if u == "" {
		return errors.New("missing URL")
	}
	uu, err := url.Parse(u)
	if err != nil {
		return errors.Wrap(err, "invalid URL")
	}
	if !uu.IsAbs() {
		return errors.New("should be absolute URL")
	}
	if uu.Scheme == "" {
		return errors.New("missing protocol (use http:// or https://)")
	}
	return nil
}

func reverseProxy(target string) *httputil.ReverseProxy {
	u, err := url.Parse(target)
	if err != nil {
		panic(err)
	}
	return &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			r.URL.Scheme = u.Scheme
			r.URL.Host = u.Host
			r.URL.Path = u.Path
		},
	}
}

const consoleHTML = `<html>
<head>
	<title>Test console - Veritone Engine Toolkit</title>
	<link href="https://fonts.googleapis.com/css?family=Roboto|Anonymous+Pro" rel="stylesheet">
	<link rel='stylesheet' href='https://machinebox.io/veritone/engine-toolkit/static/bulma.min.css'>
	<link rel='stylesheet' href='https://machinebox.io/veritone/engine-toolkit/static/docs.css?v=1'>
	<style>
		.veritone.hero {
			background: url(https://machinebox.io/veritone/engine-toolkit/static/VG@1x.png) rgb(86, 131, 163) no-repeat;
			background-size: contain;
			background-position: left;
			margin-bottom: 20px;
		}
		.padded {
			margin-top: 20px;
		}
		.footer {
			margin-top: 100px;
		}
		.padding {
			padding: 20px;
			margin-bottom: 0;
		}
		pre {
			padding: 5px;
			tab-size: 2;
			background-color: #f5f5f5;
		}

		.status.tag {
			float: right;
			border-radius: 0px;
		}
	</style>
</head>
<body>
	<section class='hero veritone is-dark'>
		<div class='hero-body'>
			<div class='container'>
				<a href='https://machinebox.io/veritone/engine-toolkit' style='float:right;'>Project homepage</a>
				<h1 class='title'>
					<a href='/'>Veritone Engine Toolkit</a>
					<span class='tag is-warning'>BETA</span>
				</h1>
				<h2 class='subtitle'>
					TEST CONSOLE
				</h2>
			</div>
		</div>
	</section>
	<section class='container'>
		<div class='columns'>
			<div class='column is-two-thirds'>
				<div class='content'>
					<p>
						Welcome to the Engine Toolkit Test Console.
						You can use this tool to test the HTTP webhooks that you must implement
						in order to deploy an engine to the aiWARE platform.
					</p>
					<p>
						Jump to a specific section from the list below, or read this document
						from the top for a guided tour.
					</p>
					<ul>
						<li>
							<a href='#Introduction'>Introduction</a>
						</li>
						<li>
							<a href='#Manifest file'>Manifest file</a> <span style='display:none' class='tag is-warning only-when-manifest-not-ok'>!</span>
						</li>
						<li>
							<a href='#Environment variables'>Environment variables</a> <span style='display:none' class='tag is-warning only-when-env-vars-not-ok'>!</span>
						</li>
						<li>
							<a href='#Ready webhook test'>Ready webhook test</a> <span style='display:none' class='tag is-warning only-when-not-ready'>!</span>
						</li>
						<li>
							<a href='#Process webhook test'>Process webhook test</a>
						</li>
						<li>
							<a href='#Best practices and advice'>Best practices and advice</a>
						</li>
						<li>
							<a href='#What next'>What next?</a>
						</li>
					</ul>
					<h1 id='Introduction' class='padded'>
						Introduction
					</h1>
					<p>
						The goal of testing your engine is to ensure it is working as expected before it
						is deployed to the aiWARE platform.
					</p>
					<p>
						This tool makes the same HTTP requests to your engine as the Engine Toolkit will
						at runtime in production. This allows you to see your engine in action, to make sure
						it performs as expected and giving you the confidence to push it into production.
					</p>
					<p>
						In fact, the Engine Toolkit Test Console has already started testing your engine's
						<a href='#Ready webhook test'>Ready webhook</a>.
					</p>
					<h1 id='Manifest file'>Manifest file</h1>
					<p>
						The <a target='_blank' href='https://machinebox.io/veritone/engine-toolkit#manifest-file'>Manifest file</a> is required to be
						added to your Docker container at <code>/var/manifest.json</code>.
					</p>
					<p class='padding'>
						<span style='display:none;' class='tag is-medium is-danger manifest-error only-when-manifest-not-ok'></span>
						<span style='display:none;' class='tag is-medium is-success only-when-manifest-ok'>LOOKS GOOD</span>
					</p>
					<h1 id='Environment variables'>Environment variables</h1>
					<p>
						<a target='_blank' href='https://machinebox.io/veritone/engine-toolkit#webhook-environment-variables'>Environment variables</a> are used to tell the Engine Toolkit about the Webhooks you have
						implemented in your engine.
					</p>
					<p class='padding'>
						<span style='display:none;' class='tag is-medium is-danger env-vars-error only-when-env-vars-not-ok'></span>
						<span style='display:none;' class='tag is-medium is-success only-when-env-vars-ok'>DETECTED</span>
					</p>
					<h1 id='Ready webhook test'>Ready webhook test</h1>
					<p>
						This tool is periodically pinging your engine's <a href='https://machinebox.io/veritone/engine-toolkit#ready-webhook'>Ready webhook</a>
						waiting for a <code>200 OK</code> response. The following badge will go green when your engine is ready:
					</p>
					<p class='padding'>
						<a style='display:none;' target='_blank' href='https://machinebox.io/veritone/engine-toolkit#ready-webhook' class='tag is-medium is-danger only-when-not-ready'>NOT READY</a>
						<a style='display:none;' target='_blank' href='https://machinebox.io/veritone/engine-toolkit#ready-webhook' class='tag is-medium is-success only-when-ready'>READY</a>
					</p>
					<p>
						Once the Ready webhook returns a successful response, the endpoint will not be checked
						again. This is the same behavior as the production platform.
					</p>
					<h1 id='Process webhook test'>Process webhook test</h1>
					<p>
						Once you have implemented the <a href='https://machinebox.io/veritone/engine-toolkit#process-webhook'>Process webhook</a>
						you can initiate tests using this console.
					</p>
				</div>
				<form data-output='#process-output' method='post' action='/api/engine/process' enctype='multipart/form-data'>
					<div class='columns'>
						<div class='column is-two-thirds'>
							<div class='field'>
								<label class='label'>Chunk file <code>chunk</code></label>
								<div class='control'>
									<input name='chunk' type='file' class='input'>
								</div>
								<p class='help'>
									e.g. choose an image (the frame of a video) or a short audio clip.
								</p>
							</div>
						</div>
						<div class='column'>
							<div class='field'>
								<label class='label'>MIME type</label>
								<div class='control'>
									<input name='chunkMimeType' type='text' class='input' value='image/jpg'>
								</div>
								<p class='help'>
									<code>chunkMimeType</code>
								</p>
							</div>
						</div>
					</div>
					<div class='columns'>
						<div class='column'>
							<div class='field'>
								<label class='label'><code>startOffsetMS</code></label>
								<div class='control'>
									<input class='input' name='startOffsetMS' type='number' value='1000'>
								</div>
							</div>
						</div>
						<div class='column'>
							<div class='field'>
								<label class='label'><code>endOffsetMS</code></label>
								<div class='control'>
									<input class='input' name='endOffsetMS' type='number' value='2000'>
								</div>
							</div>
						</div>
					</div>
					<div class='columns'>
						<div class='column'>
							<div class='field'>
								<label class='label'><code>width</code></label>
								<div class='control'>
									<input class='input' name='width' type='number' step='50' value='250'>
								</div>
							</div>
						</div>
						<div class='column'>
							<div class='field'>
								<label class='label'><code>height</code></label>
								<div class='control'>
									<input class='input' name='height' type='number' step='50' value='250'>
								</div>
							</div>
						</div>
					</div>
					<p class='has-text-right'>
						<a data-opens='#advanced-form-options'>Set advanced fields&hellip;</a>
					</p>
					<div id='advanced-form-options' class='can-open'>
						<div class='columns'>
							<div class='column'>
								<div class='field'>
									<label class='label'><code>libraryID</code></label>
									<div class='control'>
										<input class='input' name='libraryID' type='text'>
									</div>
								</div>
							</div>
							<div class='column'>
								<div class='field'>
									<label class='label'><code>libraryEngineModelId</code></label>
									<div class='control'>
										<input class='input' name='libraryEngineModelId' type='text'>
									</div>
								</div>
							</div>
						</div>
						<div class='field'>
							<label class='label'><code>cacheURI</code></label>
							<div class='control'>
								<input class='input' name='cacheURI' type='url'>
							</div>
						</div>
						<div class='columns'>
							<div class='column'>
								<div class='field'>
									<label class='label'><code>veritoneApiBaseUrl</code></label>
									<div class='control'>
										<input class='input' name='veritoneApiBaseUrl' type='text' value='https://api.veritone.com'>
									</div>
								</div>
							</div>
							<div class='column'>
								<div class='field'>
									<label class='label'><code>token</code></label>
									<div class='control'>
										<input class='input' name='token' type='text'>
									</div>
								</div>
							</div>
						</div>
						<div class='field'>
							<label class='label'><code>payload</code></label>
							<div class='control'>
								<textarea name='payload' class='textarea' placeholder='Paste JSON payload here'>{
	"applicationId": "applicationId",
	"recordingId": "recordingId",
	"jobId": "jobId",
	"taskId": "taskId",
	"token": "token",
	"mode": "mode",
	"libraryId": "libraryId",
	"libraryEngineModelId": "libraryEngineModelId",
	"veritoneApiBaseUrl": "https://api.veritone.com"
}</textarea>
							</div>
						</div>
					</div>
					<br>
					<div class='field'>
						<p class='control'>
							<button class='button is-primary only-when-ready'>Submit request</button>
							<button class='button is-primary only-when-not-ready' disabled data-tooltip='You can submit requests once the engine is ready'>Submit request</button>
						</p>
					</div>
					<div class='notification is-warning only-when-not-ready'>
						You cannot test the Process webhook until your engine is ready.
					</div>
					<div id='process-output'>
						<span class='tag status'></span>
						<pre class='body'>The response details will be displayed here after you submit a request.</pre>
					</div>
				</form>
				<br>
				<section class='content'>
					<h1 id='Best practices and advice'>Best practices and advice</h1>
					<p>
						The following advice has helped us, and others, successfully test and deploy 
						their engines into production:
					</p>
					<ul>
						<li>
							In the terminal where you executed <code>docker run</code>, you will also see 
							any log output from the Engine Toolkit and your own engine.
						</li>
						<li>
							Only worry about the fields that your engine actually uses. There's no need to test every 
							parameter in the test requests. You can safely ignore any that are
							not relevant to your use case.
						</li>
						<li>
							It is recommended that you automate your testing where possible, rather than rely on
							performing manual testing with this console. You can make assertions programtically 
							about the response of your webhooks. See <a href='#Using the API'>Using the API</a>.
						</li>
					</ul>
				</section>
				<section class='content'>
					<h1 id='What next'>What next?</h1>
					<p>
						Once your engine is complete and fully tested, you can <a href='https://machinebox.io/veritone/engine-toolkit#deploy-to-veritone'>deploy it into the Veritone aiWARE platform</a>.
					</p>
				</section>
			</div>
			<div class='column'>
				<p>
					<strong>This is a BETA release</strong> of the Engine Toolkit SDK.
					While there are engines currently running in production built using this toolkit,
					some features might be changed in backward-incompatible ways and are not subject to any SLA or deprecation policy.
				</p>
			</div>
		</div>
	</section>
	<footer class="footer">
		<div class="content has-text-centered">
			<p>
				<small>
					Copyright &copy;2019 Veritone, Inc. All rights reserved.
				</small>
			</p>
		</div>
	</footer>
	<script src='https://machinebox.io/veritone/engine-toolkit/static/jquery-3.3.1.min.js'></script>
	<script>

		$(function(){
			$('.can-open').hide()
			$('[data-opens]').click(function(e){
				e.preventDefault()
				var $link = $(this)
				$link.css({visibility:'hidden'})
				$($link.attr('data-opens')).slideDown()
			})

			$.ajax({
				method: 'get', url: '/api/engine/manifest.json',
				success: function(){
					$('.only-when-manifest-ok').show()
					$('.only-when-manifest-not-ok').hide()
					console.info(arguments)
				},
				error: function(xhr){
					$('.only-when-manifest-ok').hide()
					$('.only-when-manifest-not-ok').show()
					$('.manifest-error').text(xhr.responseText)
				}
			})

			$.ajax({
				method: 'get', url: '/api/engine/env-vars',
				success: function(){
					$('.only-when-env-vars-ok').show()
					$('.only-when-env-vars-not-ok').hide()
					console.info(arguments)
				},
				error: function(xhr){
					$('.only-when-env-vars-ok').hide()
					$('.only-when-env-vars-not-ok').show()
					$('.env-vars-error').text(xhr.responseText)
				}
			})

			$('form').submit(function(e){
				e.preventDefault()
				var $this = $(this)
				var data = new FormData(this)
				var outputBodyEl = $($this.attr('data-output') + ' .body').empty().removeClass('has-text-danger')
				var outputStatusEl = $($this.attr('data-output') + ' .status').empty().hide()
				outputBodyEl.append('...')
				$.ajax({
					method: $this.attr('method')||'get', 
					url: $this.attr('action'),
					data: data,
					processData: false,
					contentType: false,
					success: function(response, status, xhr){
						console.info('process response:', arguments)
						if (typeof response === "string") {
							response = JSON.parse(response)
						}
						var j = JSON.stringify(response, null, '\t')
						outputBodyEl.text(j)
						outputStatusEl.text(xhr.status).attr('title', 'The Webhook replied with HTTP status code ' + xhr.status).show()
					},
					error: function(response, type, message){
						console.warn('process error:', arguments)
						var err = response.status + ' ' + message + '\n' + response.responseText
						outputBodyEl.text(err)
						outputBodyEl.addClass('has-text-danger')
						outputStatusEl.text(response.status).attr('title', 'The Webhook replied with HTTP status code ' + response.status).show()
					}
				})
			})

			var checkForReadyTimeout
			function checkForReady() {
				clearTimeout()
				$.ajax({
					method: 'get', url: '/api/engine/ready',
					success: function(){
						onReady()
					},
					error: function(){
						$('.only-when-ready').hide()
						$('.only-when-not-ready').show()
						checkForReadyTimeout = setTimeout(checkForReady, 2000)
					}
				})
			}
			checkForReady()

			function onReady() {
				$('.only-when-ready').show()
				$('.only-when-not-ready').hide()
			}

			// gentle scrolling
			// Anything with an href that points to something on the page
			// is gently scrolled to, rather than jumping.
			// Everything else is left alone.
			$("a[href]").click(function(e) { 
				var buffer = 15
				var dest = $(this).attr('href')
				dest = dest.substr(dest.indexOf('#')+1)
				var destEl = $('[id="'+dest+'"]')
				if (destEl.length > 0) {
					e.preventDefault()
					$('html,body').animate({ 
						scrollTop: destEl.offset().top - buffer
					}, 'slow')
					if (history.pushState) {
						history.pushState(null, null, '#'+dest)
					} else {
						location.hash = '#'+dest
					}
				}
			})

		})

	</script>
</body>
</html>`
