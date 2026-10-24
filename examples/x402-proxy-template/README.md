# x402-proxy-template (secrets)

Cloudflare's official template [`x402-proxy-template`](https://github.com/cloudflare/templates/tree/dfdf14099a81a08cfe1ffc78f8463605670ae513/x402-proxy-template),
as it is (pinned at commit `dfdf1409`, MIT License, see `LICENSE`; the template's own description is in `README.cloudflare.md`): a proxy charging by the x402 protocol, where a visitor who has paid gets
a cookie signed with the secret `JWT_SECRET`. It uses:

- **secrets**: `JWT_SECRET` is written in `.dev.vars.example` (`cowctl deploy` reads only the names in this file, never `.dev.vars`);
- static assets (`public/`), `vars`, npm dependencies (`hono`, `x402-hono`), built by the template's own wrangler.

It serves at `/` of a hostname of its own, which the administrator registers to the tenant first. With a tenant and its credential as the quickstart's section 2 makes them
(the tenant `acme`, the admin credential in `~/.cowctl/admin`, the tenant's in `~/.cowctl/token`):

```bash
npm ci                                                        # dependencies (about 600 packages)
COWCTL_TOKEN=$(cat ~/.cowctl/admin) cowctl tenant update acme --hostname x402.localhost   # the list replaces acme's: name every hostname it keeps
cowctl init --tenant acme --route x402.localhost/
export JWT_SECRET=$(node -e 'console.log(require("crypto").randomBytes(32).toString("hex"))')
printf '%s' "$JWT_SECRET" | cowctl secret put JWT_SECRET    # the value is read only from standard input
cowctl deploy                                              # declares JWT_SECRET (refused without a value set); waits for the nodes
curl http://x402.localhost:18080/__x402/health                # {"status":"ok",...}
curl -i http://x402.localhost:18080/__x402/protected          # 402: not paid and no cookie
```

You can verify that the Worker reads the secret without paying: sign a token yourself with the same value, and the protected path answers 200 with it:

```bash
TOKEN=$(node -e 'const c=require("crypto"),b=s=>Buffer.from(s).toString("base64url"),now=Math.floor(Date.now()/1000);
const h=b(JSON.stringify({alg:"HS256",typ:"JWT"})),p=b(JSON.stringify({paid:true,iat:now,exp:now+3600}));
console.log(h+"."+p+"."+c.createHmac("sha256",process.env.JWT_SECRET).update(h+"."+p).digest("base64url"))')
curl --cookie "auth_token=$TOKEN" http://x402.localhost:18080/__x402/protected   # {"message":"Premium content accessed!",...}
```

Changing the secret: `secret put JWT_SECRET` again. Isolates loaded after that use the new value; loaded ones keep the old value until they are reclaimed. `cowctl secret list` lists only names.
