import { useState } from "react";
import type { NewS3Credential } from "../api/types";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";

interface SecretBannerProps {
  credential: NewS3Credential;
  onDismiss: () => void;
}

export function SecretBanner({ credential, onDismiss }: SecretBannerProps) {
  const [copied, setCopied] = useState(false);

  function handleCopy() {
    navigator.clipboard.writeText(credential.secretKey).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }

  return (
    <Alert className="border-amber-500 bg-amber-950/30">
      <AlertTitle className="text-amber-400 font-semibold">
        New credential created. Save your secret key now.
      </AlertTitle>
      <AlertDescription className="mt-3 flex flex-col gap-3">
        <p className="text-amber-300/80 text-sm">This key will not be shown again.</p>
        <div className="flex flex-col gap-2">
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground w-24 shrink-0">Access Key ID</span>
            <code className="font-mono text-xs bg-muted px-2 py-1 rounded">{credential.accessKeyId}</code>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground w-24 shrink-0">Secret Key</span>
            <code className="font-mono text-xs bg-muted px-2 py-1 rounded break-all">{credential.secretKey}</code>
            <Button variant="outline" size="sm" onClick={handleCopy}>
              {copied ? "Copied!" : "Copy"}
            </Button>
          </div>
        </div>
        <Button onClick={onDismiss} className="self-start mt-1">
          I have saved this key
        </Button>
      </AlertDescription>
    </Alert>
  );
}
