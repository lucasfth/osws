export interface UserProfile {
  id: number;
  name: string;
  email: string | null;
  provider: string;
  roles: Role[];
  isAdmin: boolean;
}

export interface S3Credential {
  id: number;
  accessKeyId: string;
  isActive: boolean;
  createdAt: string;
  defaultRoleId: number | null;
  defaultRoleName: string | null;
}

export interface NewS3Credential extends S3Credential {
  secretKey: string;
}

export interface Role {
  id: number;
  name: string;
}

export interface UserWithRoles {
  id: number;
  name: string;
  email: string | null;
  roles: Role[];
}

export interface ColumnWithRoles {
  id: number;
  name: string;
  roles: Role[];
}
